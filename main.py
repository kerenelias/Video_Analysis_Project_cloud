# [import]
import os
from google.cloud import videointelligence
from google.cloud import storage
from google.cloud.storage import Blob

# [START transfer_files_to_result_bucket_func]
def transfer_files_to_result_bucket(event,context):
    file = event
    file_name = file['name']

    storage_client = storage.Client()
    source_bucket = storage_client.get_bucket('transfer_from_on-prem')
    destination_bucket = storage_client.get_bucket('result_videointelligence')
    
    blobs=list(source_bucket.list_blobs())
    print(blobs)

    for blob in blobs:
        if blob.name == file_name:
            source_blob = source_bucket.blob(blob.name)
            new_blob = source_bucket.copy_blob(source_blob, destination_bucket, blob.name) 
            print (f'File moved from {source_blob} to {new_blob}')
            source_blob.delete()
        else:
            print ('File size is below IMB"')
# [END transfer_files_to_result_bucket_func]


# [START videointelligence_func]
def videointelligence_func(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.

    All Video Intelligence API features run on a video stored on GCS.
    """

    gcs_uri = "gs://transfer_from_on-prem/"+event["name"]

    output_uri = "gs://result_videointelligence/output - {}.json".format(event["name"])
    
    video_client = videointelligence.VideoIntelligenceServiceClient()

    features = [
        videointelligence.Feature.OBJECT_TRACKING,
        videointelligence.Feature.LABEL_DETECTION,
        videointelligence.Feature.SHOT_CHANGE_DETECTION,
        videointelligence.Feature.SPEECH_TRANSCRIPTION,
        videointelligence.Feature.LOGO_RECOGNITION,
        videointelligence.Feature.EXPLICIT_CONTENT_DETECTION,
        videointelligence.Feature.TEXT_DETECTION,
        videointelligence.Feature.FACE_DETECTION,
        videointelligence.Feature.PERSON_DETECTION
    ]

    transcript_config = videointelligence.SpeechTranscriptionConfig(
        language_code="en-US", enable_automatic_punctuation=True
    )

    person_config = videointelligence.PersonDetectionConfig(
        include_bounding_boxes=True,
        include_attributes=False,
        include_pose_landmarks=True,
    )

    face_config = videointelligence.FaceDetectionConfig(
        include_bounding_boxes=True, include_attributes=True
    )


    video_context = videointelligence.VideoContext(
        speech_transcription_config=transcript_config,
        person_detection_config=person_config,
        face_detection_config=face_config)

    operation = video_client.annotate_video(
        request={"features": features,
                "input_uri": gcs_uri,
                "output_uri": output_uri,
                "video_context": video_context}
    )

    print("\nProcessing video.", operation)

    transfer_files_to_result_bucket(event,context)

    result = operation.result(timeout=300)

    print("\n finnished processing.")  
# [END videointelligence_func]


# [START functions_storage_trigger_func]
def storage_trigger_func(event, context):
    file = event['name']
    split_file = os.path.splitext(file)
    file_type = split_file[-1]
    try:
        # Check if the file is video
        if file_type == '.mp4':
            videointelligence_func(event,context)

    except TypeError:
        print("Only video files are allowed")

# [END functions_storage_trigger_func]