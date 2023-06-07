###Base image for pipeline

Build image containing python packages based on Dockerfile.

The image name is 'pubsub-streaming-base-image:tag1'. If changing the name to something else, please make sure you are also changing it in the script.

Deploy to 'europe-west2-docker.pkg.dev/prj-ext-dev-faang-gcp-dr/pubsub-streaming-docker-repo/'

**Command:**

*gcloud builds submit --region=europe-west2 --tag europe-west2-docker.pkg.dev/prj-ext-dev-faang-gcp-dr/pubsub-streaming-docker-repo/pubsub-streaming-base-image:tag1 --project=prj-ext-dev-faang-gcp-dr*

###Launching the Dataflow pipeline

Simply run the script and this will launch a Dataflow pipeline job.

**Command:**

*python pubsub_streaming.py*




