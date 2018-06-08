package com.renault.datalake.openday.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.joda.time.Instant;

import java.io.IOException;

@DefaultCoder(AvroCoder.class)
public class BeaconSniffer {
    public String snifferAddr;
    public String advAddr;
    public Instant datetime;

    private static String JSON_PATTERN = "{\"sniffer_addr\": \"%s\", \"adv_addr\": \"%s\", \"datetime\": %f}";

    public String toJson() {
        return String.format(JSON_PATTERN, snifferAddr, advAddr, datetime.getMillis() / 1000.0);
    }

    public static BeaconSniffer fromJson(String json) {
        try {
            JsonNode obj = new ObjectMapper().readTree(json);
            BeaconSniffer bs = new BeaconSniffer();

            bs.snifferAddr = obj.get("sniffer_addr").asText();
            bs.advAddr = obj.get("adv_addr").asText();
            bs.datetime = new Instant((long) (obj.get("datetime").asDouble() * 1000L));

            return bs;
        } catch (IOException e) {
            return null;
        }
    }
}
