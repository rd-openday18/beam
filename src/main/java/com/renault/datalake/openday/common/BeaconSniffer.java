package com.renault.datalake.openday.common;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.joda.time.Instant;

@DefaultCoder(AvroCoder.class)
public class BeaconSniffer {
    public String snifferAddr;
    public String advAddr;
    public Instant datetime;
}
