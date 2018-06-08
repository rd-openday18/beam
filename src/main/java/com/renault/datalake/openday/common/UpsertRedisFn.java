package com.renault.datalake.openday.common;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class UpsertRedisFn extends DoFn<KV<String, BeaconSniffer>, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(UpsertRedisFn.class);
    private static final long EXPIRE_TIME_MS = 3 * 60 * 1000;

    private String redisHost;
    private Integer redisPort;

    private transient Jedis jedis;

    public UpsertRedisFn(String redisHost, Integer redisPort) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }

    @Setup
    public void setup() {
        jedis = new Jedis(redisHost, redisPort);
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) {
        KV<String, BeaconSniffer> record = processContext.element();

        String value = jedis.get(record.getKey());
        if (value == null) {
            jedis.set(record.getKey(), record.getValue().toJson());
            jedis.expireAt(record.getKey(), System.currentTimeMillis() + EXPIRE_TIME_MS);
            return;
        }

        BeaconSniffer curBs = BeaconSniffer.fromJson(value);
        if (curBs == null)
            return;

        BeaconSniffer newBs = record.getValue();

        if (curBs.datetime.isBefore(newBs.datetime)) {
            LOG.info(String.format("Setting key %s at %s from %s to %s",
                    newBs.advAddr, newBs.datetime.toString(), curBs.snifferAddr, newBs.snifferAddr));
            jedis.set(record.getKey(), newBs.toJson());
            jedis.expireAt(record.getKey(), System.currentTimeMillis() + EXPIRE_TIME_MS);
        }
    }

    @Teardown
    public void teardown() {
        jedis.close();
    }
}
