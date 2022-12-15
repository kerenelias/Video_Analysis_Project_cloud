# [import]
import os
from google.cloud import videointelligence

# # [START videointelligence_func]
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