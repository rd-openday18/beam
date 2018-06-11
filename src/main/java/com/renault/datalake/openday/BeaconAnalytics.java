package com.renault.datalake.openday;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.renault.datalake.openday.common.BeaconSniffer;
import com.renault.datalake.openday.common.Message;
import com.renault.datalake.openday.common.UpsertRedisFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.*;


public class BeaconAnalytics {

    public interface Options extends PipelineOptions {
        @Description("Google Cloud project id")
        String getGoogleCloudProjectId();

        void setGoogleCloudProjectId(String value);

        @Description("Google PubSub subscription name")
        @Default.String("")
        String getGooglePubsubSubscription();

        void setGooglePubsubSubscription(String value);

        @Description("Google BigQuery dataset name")
        String getGoogleBigqueryDataset();

        void setGoogleBigqueryDataset(String value);

        @Description("Google BigQuery table name")
        String getGoogleBigqueryTable();

        void setGoogleBigqueryTable(String value);

        @Description("Redis hostname")
        String getRedisHostname();

        void setRedisHostname(String value);

        @Description("Redis port")
        String getRedisPort();

        void setRedisPort(String value);
    }

    static class DeserializeFn extends DoFn<PubsubMessage, Message> {
        // TODO: transform to MapElements ?

        private static final Logger LOG = LoggerFactory.getLogger(DeserializeFn.class);

        @ProcessElement
        public void processElement(ProcessContext c) {
            String json = new String(c.element().getPayload());
            Message msg;
            try {
                msg = Message.fromJson(json);
            } catch (Exception exc) {
                LOG.warn("Unable to serialize message: {}", json, exc);
                return;
            }
            try {
                if (msg != null)
                    c.outputWithTimestamp(msg, msg.datetime);
            } catch (Exception exc) {
                LOG.warn("Unable to output timestamped message: {}", json, exc);
            }
        }

        // TODO: check if there is a way to stamp message when publishing to Google PubSub
        @Override
        public Duration getAllowedTimestampSkew() {
            return Duration.millis(Long.MAX_VALUE);
        }
    }

    static class FilterFn extends DoFn<Message, Message> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Message msg = c.element();
            boolean isBeacon = msg.advertiserAddr.startsWith("18:7a:93:");
            boolean isRaspberry = msg.snifferAddr.startsWith("b8:27:eb:");
            if (isBeacon && isRaspberry) {
                c.output(msg);
            }
        }
    }

    static class KeyByAdvertiserFn extends DoFn<Message, KV<String, Message>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Message msg = c.element();
            c.output(KV.of(msg.advertiserAddr, msg));
        }
    }

    static class PredictSnifferFn extends DoFn<KV<String, Iterable<Message>>, BeaconSniffer> {

        private String method;

        public PredictSnifferFn(String method) {
            super();
            if (!method.equals("mean") && !method.equals("median"))
                throw new IllegalArgumentException("Method must be equal to 'mean' or 'median'");
            this.method = method;
        }

        private Map<String, Double> computeMedians(Map<String, List<Double>> rssi) {
            rssi.forEach((k, v) -> Collections.sort(v));
            Map<String, Double> medians = new HashMap<>();
            for (Map.Entry<String, List<Double>> entry : rssi.entrySet()) {
                List<Double> values = entry.getValue();
                int size = values.size();
                if (size % 2 == 0) {
                    medians.put(entry.getKey(), 0.5 * (values.get(size / 2) + values.get(size / 2 - 1)));
                } else {
                    medians.put(entry.getKey(), values.get((size - 1)) / 2);
                }
            }
            return medians;
        }

        private Map<String, Double> computeMeans(Map<String, List<Double>> rssi) {
            Map<String, Double> means = new HashMap<>();
            for (Map.Entry<String, List<Double>> entry : rssi.entrySet()) {
                List<Double> values = entry.getValue();
                Double sum = 0.0;
                for (Double v : values) {
                    sum += v;
                }
                means.put(entry.getKey(), sum / values.size());
            }
            return means;
        }

        @ProcessElement
        public void processElement(ProcessContext c, BoundedWindow window) {
            // Group data by snifferAddr
            Map<String, List<Double>> rssi = new HashMap<>();
            for (Message msg : c.element().getValue()) {
                String key = msg.snifferAddr;
                Double value = msg.rssi.doubleValue();
                if (rssi.containsKey(key)) {
                    rssi.get(key).add(value);
                } else {
                    rssi.put(key, new ArrayList<>());
                    rssi.get(key).add(value);
                }
            }

            Map<String, Double> stats = null;
            if (method.equals("mean"))
                stats = computeMeans(rssi);
            else if (method.equals("median"))
                stats = computeMedians(rssi);

            // Compute the nearest snifferAddr by taking maximum
            String maxSnifferAddr = null;
            Double maxMedianSnifferAddr = Double.NEGATIVE_INFINITY;
            for (Map.Entry<String, Double> median : stats.entrySet()) {
                if (median.getValue() > maxMedianSnifferAddr) {
                    maxMedianSnifferAddr = median.getValue();
                    maxSnifferAddr = median.getKey();
                }
            }

            BeaconSniffer bs = new BeaconSniffer();
            bs.advAddr = c.element().getKey();
            bs.snifferAddr = maxSnifferAddr;
            bs.datetime = window.maxTimestamp();
            c.output(bs);
        }
    }

    static class FormatAsTableRowFn extends DoFn<BeaconSniffer, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            BeaconSniffer bs = c.element();
            TableRow row = new TableRow()
                    .set("adv_addr", bs.advAddr)
                    .set("sniffer_addr", bs.snifferAddr)
                    .set("datetime", bs.datetime.getMillis() / 1000.0);
            c.output(row);
        }
    }

    static class FormatAsRedisPairFn extends DoFn<BeaconSniffer, KV<String, BeaconSniffer>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            BeaconSniffer bs = c.element();
            c.output(KV.of(bs.advAddr, bs));
        }
    }

    static private TableSchema beaconSnifferSchema = new TableSchema().setFields(
            ImmutableList.of(
                    new TableFieldSchema().setName("adv_addr").setType("STRING"),
                    new TableFieldSchema().setName("sniffer_addr").setType("STRING"),
                    new TableFieldSchema().setName("datetime").setType("TIMESTAMP")
            )
    );

    public static void main(String[] args) {
        // Command line options
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        String projectId = options.getGoogleCloudProjectId().trim();
        String subscription = String.format("projects/%s/subscriptions/%s",
                projectId, options.getGooglePubsubSubscription().trim());
        String dataset = options.getGoogleBigqueryDataset().trim();
        String table = options.getGoogleBigqueryTable().trim();
        String redisHostname = options.getRedisHostname().trim();
        Integer redisPort = Integer.parseInt(options.getRedisPort().trim());

        JedisPool pool = new JedisPool(new JedisPoolConfig(), redisHostname, redisPort);

        // Pipeline
        Pipeline p = Pipeline.create(options);

        PubsubIO.Read<PubsubMessage> reader = PubsubIO
                .readMessagesWithAttributes()
                .fromSubscription(subscription);

        PCollection<Message> input = p.apply(reader)
                .apply(ParDo.of(new DeserializeFn()))
                .apply(ParDo.of(new FilterFn()));

        PCollection<Message> windowedInput = input.apply(
                Window.into(FixedWindows.of(Duration.standardSeconds(10))));

        PCollection<KV<String, Iterable<Message>>> groupByAdvertiser = windowedInput
                .apply(ParDo.of(new KeyByAdvertiserFn()))
                .apply(GroupByKey.create());

        PCollection<BeaconSniffer> nearestSniffer = groupByAdvertiser
                .apply(ParDo.of(new PredictSnifferFn("mean")));

        // Write to BigQuery
        String tableUri = projectId + ":" + dataset + "." + table;
        nearestSniffer
                .apply(ParDo.of(new FormatAsTableRowFn()))
                .apply(
                        BigQueryIO
                                .writeTableRows()
                                .to(tableUri)
                                .withSchema(beaconSnifferSchema)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        // Write to Redis
        nearestSniffer
                .apply(ParDo.of(new FormatAsRedisPairFn()))
                .apply(ParDo.of(new UpsertRedisFn(redisHostname, redisPort)));

        // Run
        p.run().waitUntilFinish();
    }
}
