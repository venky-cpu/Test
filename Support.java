package com.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;

import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

// ─────────────────────────────────────────────────────────────────────────────
// PipelineConfig — all configuration in one place, loaded from env vars
// ─────────────────────────────────────────────────────────────────────────────

record PipelineConfig(
        String bucket,
        String prefix,
        String jsonFileName,
        String awsRegion,
        String esHost,
        int    esPort,
        String esIndex,
        String esUsername,
        String esPassword,
        String checkpointPath,
        String dlqPath
) {
    static PipelineConfig fromEnv() {
        return new PipelineConfig(
                env("S3_BUCKET",         "my-bucket"),
                env("S3_PREFIX",         "testqueue/"),
                env("JSON_FILE_NAME",    "data.json"),     // nested key name inside each prefix
                env("AWS_REGION",        "us-east-1"),
                env("ES_HOST",           "localhost"),
                Integer.parseInt(env("ES_PORT", "9200")),
                env("ES_INDEX",          "testqueue-index"),
                env("ES_USERNAME",       "elastic"),
                env("ES_PASSWORD",       "changeme"),
                env("CHECKPOINT_PATH",   "/tmp/s3-pipeline-checkpoint.txt"),
                env("DLQ_PATH",          "/tmp/s3-pipeline-dlq.jsonl")
        );
    }

    private static String env(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// CheckpointStore — persists S3 continuation token for crash-safe resume
// ─────────────────────────────────────────────────────────────────────────────

class CheckpointStore {

    private static final Logger log = LoggerFactory.getLogger(CheckpointStore.class);
    private final Path path;

    CheckpointStore(String filePath) {
        this.path = Path.of(filePath);
    }

    /** Load the last saved continuation token, or null if none. */
    String load() {
        try {
            if (Files.exists(path)) {
                String token = Files.readString(path).strip();
                return token.isEmpty() ? null : token;
            }
        } catch (IOException e) {
            log.warn("Could not read checkpoint: {}", e.getMessage());
        }
        return null;
    }

    /** Atomically save the continuation token after each S3 page. */
    void save(String token) {
        try {
            Files.writeString(path, token,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            log.warn("Could not save checkpoint: {}", e.getMessage());
        }
    }

    /** Remove checkpoint on successful completion so next run starts fresh. */
    void clear() {
        try { Files.deleteIfExists(path); } catch (IOException ignored) {}
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DeadLetterQueue — captures failed keys with reasons for later reprocessing
// ─────────────────────────────────────────────────────────────────────────────

class DeadLetterQueue {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueue.class);

    record Entry(String key, String reason) {}

    private final List<Entry> entries = new CopyOnWriteArrayList<>();
    private final Path path;

    DeadLetterQueue(String filePath) {
        this.path = Path.of(filePath);
    }

    void add(String key, String reason) {
        entries.add(new Entry(key, reason));
        try (var writer = Files.newBufferedWriter(path,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            writer.write("{\"key\":\"" + key + "\",\"reason\":\"" +
                    reason.replace("\"", "'") + "\"}\n");
        } catch (IOException ignored) {}
    }

    int size() { return entries.size(); }

    void report() {
        if (entries.isEmpty()) {
            log.info("DLQ: no failures.");
        } else {
            log.warn("DLQ: {} failed keys written to {}", entries.size(), path);
            entries.stream().limit(10).forEach(e ->
                    log.warn("  key={} reason={}", e.key(), e.reason()));
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// S3ClientFactory — creates an AWS SDK v2 S3 client
// ─────────────────────────────────────────────────────────────────────────────

class S3ClientFactory {
    static S3Client create(PipelineConfig config) {
        return S3Client.builder()
                .region(Region.of(config.awsRegion()))
                .credentialsProvider(DefaultCredentialsProvider.create()) // uses env/profile/IAM role
                .build();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ElasticsearchClientFactory — creates the official Java ES client
// ─────────────────────────────────────────────────────────────────────────────

class ElasticsearchClientFactory {

    static ElasticsearchClient create(PipelineConfig config) {
        var credProvider = new BasicCredentialsProvider();
        credProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(config.esUsername(), config.esPassword()));

        RestClient restClient = RestClient
                .builder(new HttpHost(config.esHost(), config.esPort(), "http"))
                .setHttpClientConfigCallback(builder ->
                        builder.setDefaultCredentialsProvider(credProvider))
                .build();

        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        return new ElasticsearchClient(transport);
    }
}
