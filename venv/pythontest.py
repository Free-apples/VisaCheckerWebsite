import base64
from google.cloud import bigquery
from google.cloud import pubsub_v1
from google.cloud import storage


def unsubscribe(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    project_id = "group2project"
    topic_id = "unsubscribeTopic"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    # Access information in bucket, and then delete information in bucket.
    client = storage.Client()
    emailBlob = bucket.get_blob('EMAILS')
    bucket = client.get_bucket('vb-function-vars')

    emails = str((emailBlob.download_as_string()).decode('utf-8').rstrip())
    path = "EMPTYEMAIL"
    newEmailBlob = bucket.blob(path)
    newEmailBlob.upload_from_string("")
    emailBlob.rewrite(newEmailBlob)
    newEmailBlob.delete()

    # Send function to topic to retry unsubscribing
    for email in emails:
        # When you publish a message, the client returns a future.
        future = publisher.publish(topic_path, email.encode("utf-8"))
