package com.renault.datalake.openday;

import com.renault.datalake.openday.common.Message;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;


public class BeaconAnalytics {

    public interface Options extends PipelineOptions {
        @Description("Google Cloud project id")
        String getGoogleCloudProjectId();
        void setGoogleCloudProjectId(String value);

        @Description("Google PubSub subscription name")
        String getGooglePubsubSubscription();
        void setGooglePubsubSubscription(String value);
    }

    static class SerializeFn extends DoFn<PubsubMessage, Message> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String json = new String(c.element().getPayload());
            try {
                Message msg = Message.fromJson(json);
                if (msg != null)
                    c.outputWithTimestamp(msg, msg.datetime);
            } catch (Exception exc) {
                // TODO: better error handling
            }
        }

        @Override
        public Duration getAllowedTimestampSkew() {
            return  Duration.millis(Long.MAX_VALUE);
        }
    }

    static class KeyBySnifferFn extends DoFn<Message, KV<String, Message>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            Message msg = c.element();
            c.output(KV.of(msg.snifferAddr, msg));
        }
    }

    static class FormatFn extends DoFn<KV<String, Long>, String> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String snifferAddr = c.element().getKey();
            Long count = c.element().getValue();
            c.output(snifferAddr + ":" + count.toString());
        }
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline p = Pipeline.create(options);

        String subscription = String.format("projects/%s/subscriptions/%s",
                options.getGoogleCloudProjectId(),
                options.getGooglePubsubSubscription());

        PubsubIO.Read<PubsubMessage> reader = PubsubIO
                .readMessagesWithAttributes()
                .fromSubscription(subscription);

        PCollection<Message> input = p.apply(reader).apply(ParDo.of(new SerializeFn()));

        PCollection<Message> windowedInput = input.apply(
                Window.into(FixedWindows.of(Duration.standardMinutes(1))));

        windowedInput
            .apply(ParDo.of(new KeyBySnifferFn()))
            .apply(Count.perKey())
            .apply(ParDo.of(new FormatFn()))
            .apply(TextIO.write().withWindowedWrites().withNumShards(1).to("/tmp/results"));

        p.run().waitUntilFinish();
    }
}
