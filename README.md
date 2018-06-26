# CTA Data Gatherer

To use:
```bash
$ dep ensure
```

To build:
```bash
$ go build -o app ./main/
```

To run: 
```bash
$ PUBSUB_TOPIC_ID=<<PUB_SUB_TOPIC_ID>> GCLOUD_PROJECT_ID=<<GCLOUD_PROJECT_ID>> ./app
```