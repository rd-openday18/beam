# beam-analytics

Run locally:
```
mvn clean compile exec:java \
    -Dexec.mainClass=com.renault.datalake.openday.BeaconAnalytics \
    -Dexec.args="--project=presence-detection-204517 \
    --googleCloudProjectId=presence-detection-204517 \
    --googlePubsubSubscription=beam \
    --googleBigqueryDataset=sniffing \
    --googleBigqueryTable=beacon_sniffer \
    --redisHostname=localhost \
    --redisPort=6379"
```

Run on Google Dataflow:
```
mvn clean compile exec:java \
    -Dexec.mainClass=com.renault.datalake.openday.BeaconAnalytics \
    -Dexec.args="--project=presence-detection-204517 \
    --region=europe-west1 \
    --maxNumWorkers=3 \
    --workerMachineType=n1-standard-2 \
    --stagingLocation=gs://sniffing/dataflow-staging \
    --gcpTempLocation=gs://sniffing/dataflow-temp \
    --googleCloudProjectId=presence-detection-204517 \
    --googlePubsubSubscription=beam \
    --googleBigqueryDataset=sniffing \
    --googleBigqueryTable=beacon_sniffer \
    --redisHostname=10.0.0.3 \
    --redisPort=6379 \
    --runner=DataflowRunner"
```
