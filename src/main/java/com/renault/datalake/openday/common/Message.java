package com.renault.datalake.openday.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.joda.time.Instant;

import java.io.IOException;


@DefaultCoder(AvroCoder.class)
public class Message {
    public Integer rssi;
    public String advertiserAddr;
    public String advertiserConstructor;
    public String snifferAddr;
    public Instant datetime;

    public static Message fromJson(String json) throws IOException {
        JsonNode obj = new ObjectMapper().readTree(json);
        Message msg = new Message();

        msg.rssi = obj.get("rssi").asInt();
        msg.advertiserAddr = obj.get("adv_addr").asText();
        msg.advertiserConstructor = obj.get("adv_constructor").asText();
        msg.snifferAddr = obj.get("sniffer_addr").asText();
        msg.datetime = new Instant((long) (obj.get("time").asDouble() * 1000L));

        return msg;
    }
}
