package com.cpbpc.rpgv2;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.util.IOUtils;
import com.cpbpc.comms.AWSUtil;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.cpbpc.comms.AWSUtil.copyFromPreviousVersion;


public class CRPGLambda implements RequestHandler<S3Event, Void> {
    
    private static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

    @Override
    public Void handleRequest(S3Event s3event, Context context) {
        for (S3Event.S3EventNotificationRecord record : s3event.getRecords()) {
            String bucketName = URLDecoder.decode(record.getS3().getBucket().getName());
            String objectKey = URLDecoder.decode(record.getS3().getObject().getKey());
            System.out.println("bucket name: " + bucketName);
            System.out.println("object key: " + objectKey);
            S3Object s3Object = s3Client.getObject(bucketName, objectKey);
            S3ObjectInputStream inputStream = s3Object.getObjectContent();

            GetObjectTaggingRequest getObjectTaggingRequest = new GetObjectTaggingRequest(bucketName, objectKey);
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
            System.out.println("start Azure!");

            try {
                String content = IOUtils.toString(inputStream);
                byte[] result = startAzure(content, tags);
                File localFile = saveMp3(result, tags);

                List<Tag> destTags = new ArrayList<>();
                destTags.add(new Tag("publish_date", tags.get("publish_date")));
                destTags.add(new Tag("voice_id", tags.get("voice_id")));
                destTags.add(new Tag("category", tags.get("category")));
                destTags.add(new Tag("pl_script", tags.get("pl_script")));
                destTags.add(new Tag("pl_script_bucket", tags.get("pl_script_bucket")));

                AWSUtil.uploadS3Object( tags.get("output_bucket"),
                        tags.get("output_prefix"),
                        tags.get("audio_key").replace(tags.get("output_prefix"), ""),
                        localFile,
                        destTags);

                System.out.println("Azure done!");
            } catch (Exception e) {
                e.printStackTrace();
            }


        }//end of for loop

        return null;
    }

    private File saveMp3(byte[] audioData, Map<String, String> tags) throws IOException {

        String audioKey = tags.get("audio_key");
        String fileParentPath = "/tmp/azure/"+audioKey.substring(0, audioKey.lastIndexOf("/"));
        checkLocalFolder(fileParentPath);
        Path path = Path.of("/tmp/azure/"+audioKey);
        Path filePath = Files.write(path, audioData);

        return filePath.toFile();
    }

    private void checkLocalFolder(String path){
        File local_audio_directory = new File(path);
        if( !local_audio_directory.exists() ){
            local_audio_directory.mkdirs();
        }
    }
    
    private byte[] startAzure(String content, Map<String, String> tags) throws InterruptedException, URISyntaxException, IOException {

//        String url = "https://southeastasia.tts.speech.microsoft.com/cognitiveservices/v1";
        String url = System.getenv("endPoint");

        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(url))
                .header("Ocp-Apim-Subscription-Key", System.getenv("speechKey"))
                .header("Content-Type", "application/ssml+xml")
                .header("X-Microsoft-OutputFormat", "audio-48khz-192kbitrate-mono-mp3")
                .header("User-Agent", "curl")
                // Add any additional headers as needed
                .POST(HttpRequest.BodyPublishers.ofString(content))
                .build();

        // Send the request and retrieve the response
        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() == 200) {
            return response.body();
        } else {
            System.out.println("Failed to retrieve MP3 file. Response code: " + response.statusCode());
        }

        return new byte[]{};
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
}

