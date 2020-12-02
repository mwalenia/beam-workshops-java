# Beam workshops template project

This repo contains the examples for Apache Beam workshops.


In order to run the pipeline on Dataflow use the command:

```
./gradlew execute -DmainClass={CLASS} \
-Dexec.args="--runner=DataflowRunner --tempLocation=gs://<temp-location>/ --region=us-central1" \
-Pdataflow-runner --info
```

