package com.cpbpc.rpgv2;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.polly.AmazonPolly;
import com.amazonaws.services.polly.AmazonPollyClientBuilder;
import com.amazonaws.services.polly.model.GetSpeechSynthesisTaskRequest;
import com.amazonaws.services.polly.model.GetSpeechSynthesisTaskResult;
import com.amazonaws.services.polly.model.OutputFormat;
import com.amazonaws.services.polly.model.StartSpeechSynthesisTaskRequest;
import com.amazonaws.services.polly.model.StartSpeechSynthesisTaskResult;
import com.amazonaws.services.polly.model.TaskStatus;
import com.amazonaws.services.polly.model.TextType;
import com.amazonaws.services.polly.model.VoiceId;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.util.IOUtils;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RPGLambda implements RequestHandler<S3Event, Void> {

    private static final AmazonPolly AMAZON_POLLY_CLIENT = AmazonPollyClientBuilder.defaultClient();
    private static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

    @Override
    public Void handleRequest(S3Event s3event, Context context) {
//        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        for (S3Event.S3EventNotificationRecord record : s3event.getRecords()) {
            String bucketName = URLDecoder.decode(record.getS3().getBucket().getName());
            String objectKey = URLDecoder.decode(record.getS3().getObject().getKey());
            System.out.println("bucket name: " + bucketName);
            System.out.println("object key: " + objectKey);
            S3Object s3Object = s3Client.getObject(bucketName, objectKey);
            S3ObjectInputStream inputStream = s3Object.getObjectContent();

            GetObjectTaggingRequest getObjectTaggingRequest = new GetObjectTaggingRequest(bucketName, objectKey);
//                    .bucket(bucketName)
//                    .key(key)
//                    .build();

            // Retrieve the object tags
            GetObjectTaggingResult getObjectTaggingResponse = s3Client.getObjectTagging(getObjectTaggingRequest);

            Map<String, String> tags = new HashMap<>();
            getObjectTaggingResponse.getTagSet().forEach(tag -> {
                tags.put(tag.getKey(), tag.getValue());
            });
            if( tags.isEmpty() ){
                tags.putAll(copyFromPreviousVersion( getObjectTaggingResponse, bucketName, objectKey ));
            }
            
            System.out.println("tags : " + tags.toString());
            System.out.println("start polly!");

            try {
                String content = IOUtils.toString(inputStream);
//                System.out.println("content : " + content);
                StartSpeechSynthesisTaskResult result = startPolly(content, tags);
                String taskId = result.getSynthesisTask().getTaskId();

                boolean end = false;
                while (!end) {
                    Thread.sleep(10000);
                    end = getSynthesisTaskStatus(taskId).equals(TaskStatus.Completed.toString());
                }

                System.out.println("polly done!");

                changeOutputName(tags,
                        result.getSynthesisTask().getOutputUri(),
                        result.getSynthesisTask().getOutputFormat(),
                        0);
            } catch (Exception e) {
                e.printStackTrace();
            }


        }//end of for loop

        return null;
    }

    private Map<String, String> copyFromPreviousVersion(GetObjectTaggingResult currentVersion, String bucketName, String objectKey) {
         Map<String, String> tags = new HashMap<>();
         String versionId = currentVersion.getVersionId();

        VersionListing list = s3Client.listVersions(bucketName, objectKey);
        List<S3VersionSummary> summaries = list.getVersionSummaries();
        for( S3VersionSummary summary : summaries ){
            if( summary.getVersionId().equals(versionId) ){
                continue;
            }
            GetObjectTaggingRequest getObjectTaggingRequest = new GetObjectTaggingRequest(bucketName, objectKey, summary.getVersionId());
            GetObjectTaggingResult getObjectTaggingResponse = s3Client.getObjectTagging(getObjectTaggingRequest);
            if( getObjectTaggingResponse.getTagSet().isEmpty() ){
                continue;
            }
            getObjectTaggingResponse.getTagSet().forEach(tag -> {
                tags.put(tag.getKey(), tag.getValue());
            });

            ObjectTagging tagging = new ObjectTagging(getObjectTaggingResponse.getTagSet());
            SetObjectTaggingRequest setObjectTaggingRequest = new SetObjectTaggingRequest(bucketName, objectKey, versionId, tagging);
            s3Client.setObjectTagging(setObjectTaggingRequest);
            break;
        }

        return tags;
    }

    private StartSpeechSynthesisTaskResult startPolly(String content, Map<String, String> tags) throws IOException {

        String engine = "neural";
        if( tags.containsKey("engine") ){
            engine = tags.get("engine");
        }

        StartSpeechSynthesisTaskRequest speechRequest = new StartSpeechSynthesisTaskRequest()
                .withOutputFormat(OutputFormat.fromValue(tags.get("output_format")))
                .withText(content)
                .withTextType(TextType.Ssml)
//                .withOutputS3BucketName("cpbpc-rpg-audio")
//                .withOutputS3KeyPrefix("audio/")
                .withOutputS3BucketName(tags.get("output_bucket"))
                .withOutputS3KeyPrefix(tags.get("output_prefix"))
//                    .withSnsTopicArn(SNS_TOPIC_ARN)
                .withVoiceId(VoiceId.fromValue(tags.get("voice_id")))
                .withEngine(engine);

        StartSpeechSynthesisTaskResult result = AMAZON_POLLY_CLIENT.startSpeechSynthesisTask(speechRequest);
        System.out.println("file url : " + result.getSynthesisTask().getOutputUri());
        return result;
    }

    private void changeOutputName(Map<String, String> tags, String outputUri,
                                  String objectType, int count) {
        System.out.println("ready to change name");

        if (null == outputUri || outputUri.trim().length() <= 0) {
            return;
        }

        String publishDate_str = tags.get("publish_date");
//        String publish_month = publishDate_str.split("-")[0] + "_" + publishDate_str.split("-")[1];
//        nameToBe = publishDate_str + "_" + nameToBe.replaceAll(" ", "-")
//                + "_" + voiceId
//        String nameToBe = tags.get("name_prefix") + publishDate_str.replaceAll("-", "");
//        if (count > 0) {
//            nameToBe += "-" + count;
//        }
        ;

        String bucketName = tags.get("output_bucket");
        String prefix = tags.get("output_prefix");
        if (!prefix.endsWith("/")) {
            prefix += "/";
        }

        String[] pathInfo = outputUri.split("/");
        String objectName = pathInfo[pathInfo.length - 1];

//        String destination_key = prefix + publish_month + "/" + nameToBe + "." + objectType;
        String destination_key = tags.get("audio_key");
        CopyObjectRequest copyObjRequest = new CopyObjectRequest(bucketName,
                                                        prefix + objectName,
                                                                bucketName,
                                                                destination_key);
//        ObjectMetadata metadata = new ObjectMetadata();
//        metadata.addUserMetadata("Content-Type", "audio/mpeg");
//        metadata.addUserMetadata("needPL", "true");
//        copyObjRequest.setNewObjectMetadata(metadata);

        List<Tag> destTags = new ArrayList<>();
        destTags.add(new Tag("publish_date", publishDate_str));
        destTags.add(new Tag("voice_id", tags.get("voice_id")));
        destTags.add(new Tag("category", tags.get("category")));
        destTags.add(new Tag("engine", tags.get("engine")));
        if( tags.containsKey("pl_script") && null != tags.get("pl_script") ){
            destTags.add(new Tag("pl_script", tags.get("pl_script")));
        }
        if( tags.containsKey("pl_script_bucket") && null != tags.get("pl_script_bucket") ){
            destTags.add(new Tag("pl_script_bucket", tags.get("pl_script_bucket")));
        }

        copyObjRequest.setStorageClass(StorageClass.IntelligentTiering);
        copyObjRequest.setNewObjectTagging(new ObjectTagging(destTags));
        s3Client.copyObject(copyObjRequest);

        System.out.println("delete this object : " + prefix + objectName);
        s3Client.deleteObject(new DeleteObjectRequest(bucketName, prefix + objectName));

    }

    private String getSynthesisTaskStatus(String taskId) {
        GetSpeechSynthesisTaskRequest getSpeechSynthesisTaskRequest = new GetSpeechSynthesisTaskRequest()
                .withTaskId(taskId);
        GetSpeechSynthesisTaskResult result = AMAZON_POLLY_CLIENT.getSpeechSynthesisTask(getSpeechSynthesisTaskRequest);
        System.out.println("polly status : " + result.getSynthesisTask().getTaskStatus());

        if (TaskStatus.Failed.toString().equals(result.getSynthesisTask().getTaskStatus())) {
            System.out.println(result.getSynthesisTask().getTaskStatusReason());
        }
        return result.getSynthesisTask().getTaskStatus();
    }
}

