package com.renault.datalake.openday.common;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class UpsertRedisFn extends DoFn<KV<String, String>, Void> {

    private static final int DEFAULT_BATCH_SIZE = 10;
    private static final long EXPIRE_TIME_MS = 3 * 60 * 1000;

    private String redisHost;
    private Integer redisPort;
    private Long now;

    private transient Jedis jedis;
    private transient Pipeline pipeline;

    private int batchCount;

    public UpsertRedisFn(String redisHost, Integer redisPort) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }

    @Setup
    public void setup() {
        jedis = new Jedis(redisHost, redisPort);
    }

    @StartBundle
    public void startBundle() {
        now = System.currentTimeMillis();
        pipeline = jedis.pipelined();
        pipeline.multi();
        batchCount = 0;
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) {
        KV<String, String> record = processContext.element();
        pipeline.set(record.getKey(), record.getValue());
        pipeline.expireAt(record.getKey(), now + EXPIRE_TIME_MS);

        batchCount++;

        if (batchCount >= DEFAULT_BATCH_SIZE) {
            flush();
        }
    }

    @FinishBundle
    public void finishBundle() {
        flush();
    }

    @Teardown
    public void teardown() {
        jedis.close();
    }

    private void flush() {
        if (!pipeline.isInMulti())
            pipeline.multi();
        pipeline.exec();
        batchCount = 0;
    }
}
