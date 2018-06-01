package com.renault.datalake.openday;

import com.renault.datalake.openday.common.Message;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
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

    static class FilterBeaconsFn extends DoFn<Message, Message> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Message msg = c.element();
            if (msg.advertiserAddr.startsWith("18:7a:93:")) {
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

    static class PredictSnifferFn extends DoFn<KV<String, Iterable<Message>>, KV<String, String>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            // Group data by snifferAddr
            Map<String, List<Double>> rssi = new HashMap<>();
            for (Message msg : c.element().getValue()) {
                String key = msg.snifferAddr;
                Double value = msg.rssi.doubleValue();
                if (rssi.containsKey(key)) rssi.get(key).add(value);
                else rssi.put(key, new ArrayList<Double>(Arrays.asList(value)));
            }

            // Compute median RSSI for each snifferAddr
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

            // Compute the nearest snifferAddr by taking maximum median
            String maxSnifferAddr = null;
            Double maxMedianSnifferAddr = Double.NEGATIVE_INFINITY;
            for (Map.Entry<String, Double> median : medians.entrySet()) {
                if (median.getValue() > maxMedianSnifferAddr) {
                    maxMedianSnifferAddr = median.getValue();
                    maxSnifferAddr = median.getKey();
                }
            }
            c.output(KV.of(c.element().getKey(), maxSnifferAddr));
        }
    }

    static class FormatAsTextFn extends DoFn<KV<String, String>, String> {
        @ProcessElement
        public void processElement(ProcessContext c, BoundedWindow window) {
            c.output(c.element().getKey() + "\t" + c.element().getValue() + "\t" + window.maxTimestamp().toString());
        }
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        Pipeline p = Pipeline.create(options);

        String subscription = String.format("projects/%s/subscriptions/%s",
                options.getGoogleCloudProjectId().trim(),
                options.getGooglePubsubSubscription().trim());

        PubsubIO.Read<PubsubMessage> reader = PubsubIO
                .readMessagesWithAttributes()
                .fromSubscription(subscription);

        PCollection<Message> input = p.apply(reader)
                .apply(ParDo.of(new DeserializeFn()))
                .apply(ParDo.of(new FilterBeaconsFn()));

        PCollection<Message> windowedInput = input.apply(
                Window.into(FixedWindows.of(Duration.standardSeconds(10))));

        PCollection<KV<String, Iterable<Message>>> groupByAdvertiser = windowedInput
                .apply(ParDo.of(new KeyByAdvertiserFn()))
                .apply(GroupByKey.create());

        PCollection<KV<String, String>> nearestSniffer = groupByAdvertiser
                .apply(ParDo.of(new PredictSnifferFn()));

        nearestSniffer
                .apply(ParDo.of(new FormatAsTextFn()))
                .apply(TextIO.write().withWindowedWrites().withNumShards(1).to("/tmp/beam/nearest_sniffer_"));

        p.run().waitUntilFinish();
    }
}
