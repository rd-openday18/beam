package com.renault.datalake.openday.common;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.joda.time.Instant;

@DefaultCoder(AvroCoder.class)
public class BeaconSniffer {
    public String snifferAddr;
    public String advAddr;
    public Instant datetime;

    private static String JSON_PATTERN = "{\"sniffer_addr\": \"%s\", \"adv_addr\": \"%s\", \"datetime\": %f}";

    public String toJson() {
        return String.format(JSON_PATTERN, snifferAddr, advAddr, datetime.getMillis() / 1000.0);
    }
}
