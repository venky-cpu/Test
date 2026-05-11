package com.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lists all logical prefix keys under testqueue/ using S3 list_objects_v2.
 *
 * Strategy:
 *  - Uses Delimiter='/' so S3 returns CommonPrefixes (logical folders), not
 *    every individual file. This gives us "testqueue/key-abc/" entries directly.
 *  - Saves the S3 continuation token to CheckpointStore after every page,
 *    so a crash can resume rather than restart from zero.
 *  - Keys come back in alphabetical (lexicographic) order by default — no sorting needed.
 */
public class S3KeyLister {

    private static final Logger log = LoggerFactory.getLogger(S3KeyLister.class);

    /** Sentinel value placed in the queue to signal workers to stop. */
    public static final String POISON_PILL = "__POISON_PILL__";

    private static final int PAGE_SIZE = 1000; // S3 max per request

    private final PipelineConfig   config;
    private final CheckpointStore  checkpoint;
    private final S3Client         s3;
    private final AtomicLong       totalListed = new AtomicLong(0);

    public S3KeyLister(PipelineConfig config, CheckpointStore checkpoint) {
        this.config     = config;
        this.checkpoint = checkpoint;
        this.s3         = S3ClientFactory.create(config);
    }

    /**
     * Streams all prefix keys into the provided queue.
     * Blocks when the queue is full (natural backpressure).
     */
    public void listInto(BlockingQueue<String> keyQueue) throws InterruptedException {
        String resumeToken = checkpoint.load();
        if (resumeToken != null) {
            log.info("Resuming from checkpoint token: {}...", resumeToken.substring(0, Math.min(40, resumeToken.length())));
        }

        var requestBuilder = ListObjectsV2Request.builder()
                .bucket(config.bucket())
                .prefix(config.prefix())   // e.g. "testqueue/"
                .delimiter("/")            // logical folder grouping
                .maxKeys(PAGE_SIZE);

        if (resumeToken != null) {
            requestBuilder.continuationToken(resumeToken);
        }

        ListObjectsV2Request initialRequest = requestBuilder.build();

        // AWS SDK v2 paginator — lazy, streams pages on demand
        ListObjectsV2Iterable pages = s3.listObjectsV2Paginator(initialRequest);

        for (ListObjectsV2Response page : pages) {
            // Each CommonPrefix is one logical key, e.g. "testqueue/abc123/"
            for (CommonPrefix cp : page.commonPrefixes()) {
                keyQueue.put(cp.prefix()); // blocks if queue is full → backpressure
                long count = totalListed.incrementAndGet();
                if (count % 100_000 == 0) {
                    log.info("Listed {} keys so far...", count);
                }
            }

            // Persist continuation token after every page — crash-safe resume
            if (page.nextContinuationToken() != null) {
                checkpoint.save(page.nextContinuationToken());
            }
        }

        log.info("Listing complete. Total keys listed: {}", totalListed.get());
        // On successful completion, clear checkpoint so next run starts fresh
        checkpoint.clear();
    }
}
