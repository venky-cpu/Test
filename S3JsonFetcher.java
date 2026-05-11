package com.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fetches the JSON document nested inside each S3 prefix key.
 *
 * Given a prefix key like "testqueue/abc123/", it fetches:
 *   s3://bucket/testqueue/abc123/data.json
 *
 * Thread-safe: S3Client and ObjectMapper are both thread-safe;
 * this class can be shared across all virtual worker threads.
 */
public class S3JsonFetcher {

    private static final Logger log = LoggerFactory.getLogger(S3JsonFetcher.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final PipelineConfig config;
    private final DeadLetterQueue dlq;
    private final S3Client s3;
    private final AtomicLong successCount = new AtomicLong(0);

    public S3JsonFetcher(PipelineConfig config, DeadLetterQueue dlq) {
        this.config = config;
        this.dlq    = dlq;
        this.s3     = S3ClientFactory.create(config);
    }

    /**
     * Fetches and parses the JSON document for a given prefix key.
     *
     * @param prefixKey e.g. "testqueue/abc123/"
     * @return parsed document as a Map, or empty if fetch/parse failed
     */
    @SuppressWarnings("unchecked")
    public Optional<Map<String, Object>> fetch(String prefixKey) {
        // Construct the full key to the nested JSON file
        String jsonKey = prefixKey + config.jsonFileName(); // e.g. "testqueue/abc123/data.json"

        try {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(config.bucket())
                    .key(jsonKey)
                    .build();

            try (InputStream body = s3.getObject(request)) {
                Map<String, Object> doc = MAPPER.readValue(body, Map.class);

                // Enrich with source metadata — useful for debugging in ES
                doc.put("_s3_prefix_key", prefixKey);
                doc.put("_s3_bucket",     config.bucket());
                doc.put("_ingested_at",   System.currentTimeMillis());

                successCount.incrementAndGet();
                return Optional.of(doc);
            }

        } catch (NoSuchKeyException e) {
            // Key exists in listing but the JSON file is missing — expected in some cases
            log.warn("JSON not found for prefix: {}", prefixKey);
            dlq.add(prefixKey, "NoSuchKey: " + jsonKey);
            return Optional.empty();

        } catch (Exception e) {
            log.error("Failed to fetch/parse {}: {}", jsonKey, e.getMessage());
            dlq.add(prefixKey, e.getMessage());
            return Optional.empty();
        }
    }

    public long successCount() {
        return successCount.get();
    }
}
