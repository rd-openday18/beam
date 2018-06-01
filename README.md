# beam-analytics

Run locally:
```
mvn compile exec:java \ 
    -Dexec.mainClass=com.renault.datalake.openday.BeaconAnalytics \ -Dexec.args="--googleCloudProjectId=presence-detection-204517 --googlePubsubSubscription=beam"
```

Run on Google Dataflow:
```
mvn compile exec:java \
    -Dexec.mainClass=com.renault.datalake.openday.BeaconAnalytics \
    -Dexec.args="--project=presence-detection-204517 \
    --region=europe-west1 \
    --maxNumWorkers=3 \
    --stagingLocation=gs://sniffing/dataflow-staging \
    --gcpTempLocation=gs://sniffing/dataflow-temp \
    --googleCloudProjectId=presence-detection-204517 \
    --googlePubsubSubscription=beam \
    --googleBigqueryDataset=sniffing \
    --googleBigqueryTable=beacon_sniffer \
    --runner=DataflowRunner"
```
