package com.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * Orchestrates the full S3 → Elasticsearch pipeline.
 *
 * Phase 1: List all prefix keys under testqueue/ using S3 paginator
 * Phase 2: Fetch data.json inside each prefix key (parallel via Virtual Threads)
 * Phase 3: Bulk index into Elasticsearch with batching and retry
 */
public class S3ToEsPipeline {

    private static final Logger log = LoggerFactory.getLogger(S3ToEsPipeline.class);

    // ── Tuning knobs ──────────────────────────────────────────────────────────
    private static final int  KEY_QUEUE_CAPACITY  = 10_000;   // backpressure buffer
    private static final int  WORKER_THREADS      = 150;       // virtual threads for GetObject
    private static final int  ES_BATCH_SIZE       = 1_000;     // docs per bulk request
    private static final int  ES_FLUSH_INTERVAL_S = 5;         // flush buffer every N seconds
    // ─────────────────────────────────────────────────────────────────────────

    private final PipelineConfig          config;
    private final S3KeyLister             lister;
    private final S3JsonFetcher           fetcher;
    private final ElasticsearchBulkWriter esWriter;
    private final CheckpointStore         checkpoint;
    private final DeadLetterQueue         dlq;

    public S3ToEsPipeline(PipelineConfig config) {
        this.config     = config;
        this.checkpoint = new CheckpointStore(config.checkpointPath());
        this.dlq        = new DeadLetterQueue(config.dlqPath());
        this.lister     = new S3KeyLister(config, checkpoint);
        this.fetcher    = new S3JsonFetcher(config, dlq);
        this.esWriter   = new ElasticsearchBulkWriter(config, dlq, ES_BATCH_SIZE);
    }

    public void run() throws Exception {
        log.info("Pipeline starting — bucket={} prefix={}", config.bucket(), config.prefix());

        // Shared queue between lister (producer) and workers (consumers).
        // BlockingQueue provides natural backpressure: lister blocks when full.
        BlockingQueue<String> keyQueue = new LinkedBlockingQueue<>(KEY_QUEUE_CAPACITY);

        // ── Phase 1: Producer — list S3 keys on a dedicated virtual thread ──
        Thread producerThread = Thread.ofVirtual().name("s3-lister").start(() -> {
            try {
                lister.listInto(keyQueue);
            } catch (Exception e) {
                log.error("Lister failed", e);
            } finally {
                // Poison pills — one per worker thread signals shutdown
                for (int i = 0; i < WORKER_THREADS; i++) {
                    try { keyQueue.put(S3KeyLister.POISON_PILL); } catch (InterruptedException ignored) {}
                }
            }
        });

        // ── Phase 2 & 3: Consumer workers — fetch JSON and index into ES ──
        // Java 21 Virtual Threads: each worker is a lightweight thread; 150 costs
        // almost nothing and keeps all network I/O saturated.
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            CountDownLatch latch = new CountDownLatch(WORKER_THREADS);

            for (int i = 0; i < WORKER_THREADS; i++) {
                final int workerId = i;
                executor.submit(() -> {
                    try {
                        workerLoop(workerId, keyQueue);
                    } finally {
                        latch.countDown();
                    }
                });
            }

            producerThread.join();
            latch.await();
        }

        // Final flush of any remaining buffered docs
        esWriter.flush();
        esWriter.close();

        log.info("Pipeline complete. Processed={} Failed={}",
                fetcher.successCount(), dlq.size());
        dlq.report();
    }

    private void workerLoop(int workerId, BlockingQueue<String> keyQueue) {
        log.debug("Worker {} started", workerId);
        while (true) {
            try {
                String prefixKey = keyQueue.take();
                if (S3KeyLister.POISON_PILL.equals(prefixKey)) break;

                // Fetch the JSON document nested inside this prefix key
                fetcher.fetch(prefixKey).ifPresent(doc -> {
                    try {
                        esWriter.add(doc);
                    } catch (Exception e) {
                        dlq.add(prefixKey, "ES buffer error: " + e.getMessage());
                    }
                });

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        log.debug("Worker {} done", workerId);
    }

    // ── Entry point ───────────────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {
        PipelineConfig config = PipelineConfig.fromEnv();
        new S3ToEsPipeline(config).run();
    }
}
