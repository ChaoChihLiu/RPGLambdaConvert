package com.cpbpc.rpgv2;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.util.StringInputStream;
import com.github.difflib.DiffUtils;
import com.github.difflib.patch.AbstractDelta;
import com.github.difflib.patch.Patch;
import com.github.houbb.opencc4j.util.ZhConverterUtil;
import com.google.gson.Gson;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static com.cpbpc.comms.NumberConverter.toChineseNumber;
import static com.cpbpc.comms.OpenAIUtil.speechToText;
import static com.cpbpc.comms.PunctuationTool.removePunctuation;
import static com.cpbpc.comms.TextUtil.convertToPinyin;
import static com.cpbpc.comms.TextUtil.removeHtmlTag;
import static com.cpbpc.comms.TextUtil.removeLinkBreak;
import static com.cpbpc.comms.TextUtil.removeMultiSpace;

public class ProofListener implements RequestHandler<S3Event, Void> {

    private static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

    private static final Pattern object_key_pattern = Pattern.compile("\\d{4,4}_\\d{2,2}");
    @Override
    public Void handleRequest(S3Event s3event, Context context) {

        for (S3Event.S3EventNotificationRecord record : s3event.getRecords()) {
            String bucketName = URLDecoder.decode(record.getS3().getBucket().getName());
            String objectKey = URLDecoder.decode(record.getS3().getObject().getKey());
            System.out.println("bucket name: " + bucketName);
            System.out.println("object key: " + objectKey);

            GetObjectTaggingRequest getObjectTaggingRequest = new GetObjectTaggingRequest(bucketName, objectKey);
            GetObjectTaggingResult getObjectTaggingResponse = s3Client.getObjectTagging(getObjectTaggingRequest);

            Map<String, String> tags = new HashMap<>();
            getObjectTaggingResponse.getTagSet().forEach(tag -> {
                tags.put(tag.getKey(), tag.getValue());
            });
            if( tags.isEmpty() ){
                continue;
            }

            String audio_key = URLDecoder.decode(tags.get("audio_key"));
            String audio_bucket = URLDecoder.decode(tags.get("output_bucket"));

            //load audio first
            String audioFilePath = downloadS3Object( audio_bucket, audio_key );
            File audio = new File(audioFilePath);
            if( !audio.exists() ){
                System.out.println("audio file not exist");
                return null;
            }
            //load pl script
            String plFilePath = downloadS3Object( bucketName, objectKey );
            File plfile = new File(plFilePath);
            if( !plfile.exists() ){
                System.out.println("pl file not exist");
                return null;
            }

            String script = "";
            try {
                script = IOUtils.toString(new FileReader(plFilePath));
            } catch (IOException e) {
                e.printStackTrace();
            }

            if( StringUtils.isEmpty(script) ){
                System.out.println("cannot read script " + plFilePath);
                return null;
            }

            Gson gson = new Gson();
            Map result = gson.fromJson(speechToText(audioFilePath, "zh"), HashMap.class);
            String fromAI = ZhConverterUtil.toSimple(String.valueOf(result.get("text")));
            fromAI = removeMultiSpace(removePunctuation(fromAI));
            fromAI = fromAI.replaceAll("圣经 经文第", "圣经经文第");

            script = toChineseNumber(ZhConverterUtil.toSimple(
                    removeMultiSpace(
                            removePunctuation(
                                    removeLinkBreak(
                                            removeHtmlTag(script, ""))))));
            StringBuffer buffer = new StringBuffer();
            buffer.append("original: " + script).append(System.lineSeparator());
            buffer.append(System.lineSeparator());
            buffer.append("from AI : " + fromAI).append(System.lineSeparator());
            buffer.append(System.lineSeparator());

            Patch<String> patch = DiffUtils.diff(Arrays.asList(StringUtils.split(script, " ")), Arrays.asList(StringUtils.split(fromAI, " ")));
            for (AbstractDelta<String> delta : patch.getDeltas()) {
                String oringal = StringUtils.remove(removePunctuation(StringUtils.join(delta.getSource().getLines())), " ");
                String aiRead =  StringUtils.remove(removePunctuation(StringUtils.join(delta.getTarget().getLines())), " ");

                if( StringUtils.equals(convertToPinyin(oringal), convertToPinyin(aiRead)) ){
                    continue;
                }
                
                buffer.append("original: " + StringUtils.join(delta.getSource().getLines())).append(System.lineSeparator());
                buffer.append("from AI : " + StringUtils.join(delta.getTarget().getLines())).append(System.lineSeparator());
                buffer.append(System.lineSeparator());
            }

            saveToS3(buffer.toString(), bucketName, objectKey.replace("pl", "txt"));
        }

        return null;
    }

    private void saveToS3(String content, String bucketName, String objectKey) {
        try {
            InputStream inputStream = new StringInputStream(content);
            // Upload the file to S3
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectKey, inputStream, createS3ObjMetadata());

            putObjectRequest.setStorageClass(StorageClass.IntelligentTiering);

            s3Client.putObject(putObjectRequest);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static ObjectMetadata createS3ObjMetadata() {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setCacheControl("no-store, no-cache, must-revalidate");
        metadata.setHttpExpiresDate(new Date(0));
        return metadata;
    }

    private String downloadS3Object(String bucketName, String objectKey){

        String[] inputs = objectKey.split("/");
        String folder = "";
        for( int i = 0; i<inputs.length-1; i++ ){
            folder += inputs[i]+"/";
        }

        String localFilePath = "/tmp/"+objectKey;
        String folderPath = "/tmp/"+folder;
        System.out.println("folder path " + folderPath);
        System.out.println("file path " + localFilePath);
        try {
            File dir = new File( folderPath );
            if( !dir.exists() ){
                dir.mkdirs();
            }
            File file = new File(localFilePath);
            if( !file.exists() ){
                file.createNewFile();
            }

            S3Object s3Object = s3Client.getObject(bucketName, objectKey);

            FileOutputStream fos = new FileOutputStream(localFilePath);
            com.amazonaws.util.IOUtils.copy(s3Object.getObjectContent(), fos);
            System.out.println("Object downloaded successfully to: " + localFilePath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return localFilePath;
    }
}
