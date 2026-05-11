package com.pipeline;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread-safe Elasticsearch bulk writer.
 *
 * Buffers documents and flushes in batches for maximum throughput.
 * Uses a ReentrantLock so multiple virtual threads can call add() safely.
 *
 * Key ES performance settings to apply on your index before running:
 *   PUT /your-index/_settings
 *   { "index.refresh_interval": "-1", "index.number_of_replicas": 0 }
 * Re-enable after bulk load:
 *   { "index.refresh_interval": "1s", "index.number_of_replicas": 1 }
 */
public class ElasticsearchBulkWriter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ElasticsearchBulkWriter.class);
    private static final int MAX_RETRIES = 3;

    private final PipelineConfig       config;
    private final DeadLetterQueue      dlq;
    private final ElasticsearchClient  esClient;
    private final int                  batchSize;
    private final List<Map<String, Object>> buffer;
    private final ReentrantLock        lock = new ReentrantLock();
    private final AtomicLong           totalIndexed = new AtomicLong(0);

    public ElasticsearchBulkWriter(PipelineConfig config, DeadLetterQueue dlq, int batchSize) {
        this.config    = config;
        this.dlq       = dlq;
        this.batchSize = batchSize;
        this.buffer    = new ArrayList<>(batchSize);
        this.esClient  = ElasticsearchClientFactory.create(config);
    }

    /**
     * Add a document to the buffer. Flushes automatically when batch is full.
     * Thread-safe — multiple virtual threads call this concurrently.
     */
    public void add(Map<String, Object> doc) throws IOException {
        lock.lock();
        try {
            buffer.add(doc);
            if (buffer.size() >= batchSize) {
                flushUnderLock();
            }
        } finally {
            lock.unlock();
        }
    }

    /** Flush any remaining buffered documents. Call at end of pipeline. */
    public void flush() throws IOException {
        lock.lock();
        try {
            flushUnderLock();
        } finally {
            lock.unlock();
        }
    }

    private void flushUnderLock() throws IOException {
        if (buffer.isEmpty()) return;

        List<Map<String, Object>> batch = new ArrayList<>(buffer);
        buffer.clear();

        bulkIndexWithRetry(batch, 0);
        totalIndexed.addAndGet(batch.size());
        log.info("Indexed {} docs (total: {})", batch.size(), totalIndexed.get());
    }

    private void bulkIndexWithRetry(List<Map<String, Object>> batch, int attempt) throws IOException {
        BulkRequest.Builder req = new BulkRequest.Builder();

        for (Map<String, Object> doc : batch) {
            req.operations(op -> op
                    .index(idx -> idx
                            .index(config.esIndex())
                            .document(doc)));
        }

        BulkResponse response = esClient.bulk(req.build());

        if (response.errors()) {
            List<Map<String, Object>> failedDocs = new ArrayList<>();

            for (int i = 0; i < response.items().size(); i++) {
                BulkResponseItem item = response.items().get(i);
                if (item.error() != null) {
                    String reason = item.error().reason();

                    // 429 Too Many Requests — retry the whole batch with backoff
                    if ("too_many_requests".equals(item.error().type())) {
                        if (attempt < MAX_RETRIES) {
                            log.warn("ES 429 rate limit — retry attempt {}/{}", attempt + 1, MAX_RETRIES);
                            sleepBackoff(attempt);
                            bulkIndexWithRetry(batch, attempt + 1);
                            return;
                        }
                    }

                    log.error("ES index error for item {}: {}", i, reason);
                    failedDocs.add(batch.get(i));
                    dlq.add(String.valueOf(batch.get(i).get("_s3_prefix_key")), reason);
                }
            }
        }
    }

    private static void sleepBackoff(int attempt) {
        try {
            long ms = (long) Math.pow(2, attempt) * 1000L; // 1s, 2s, 4s
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() throws Exception {
        esClient._transport().close();
    }

    public long totalIndexed() {
        return totalIndexed.get();
    }
}
