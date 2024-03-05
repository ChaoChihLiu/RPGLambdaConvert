package com.cpbpc.rpgv2;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.Tag;
import com.cpbpc.comms.AWSUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EnFixer implements RequestHandler<S3Event, Void> {
    private static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

    @Override
    public Void handleRequest(S3Event s3event, Context context) {

        try{
            for (S3Event.S3EventNotificationRecord record : s3event.getRecords()) {
                System.out.println("event name " + record.getEventName());
                String bucketName = URLDecoder.decode(record.getS3().getBucket().getName());
                String objectKey = URLDecoder.decode(record.getS3().getObject().getKey());

                System.out.println("bucket name: " + bucketName);
                System.out.println("object key: " + objectKey);
                boolean isEnglish = StringUtils.contains(objectKey, "kjv");

                S3Object s3Object = s3Client.getObject(bucketName, objectKey);
                String csv = IOUtils.toString(s3Object.getObjectContent());

                Map<String, String> fixes = parseCSV(csv);
                String phonemePattern = genPhonemePattern(fixes);
                System.out.println("phonemePattern " + phonemePattern );
                String wordPattern = genWordPattern(fixes);
                System.out.println("wordPattern " + wordPattern );

                String prefix = StringUtils.substring(objectKey, 0, StringUtils.lastIndexOf(objectKey, "/"));
                List<S3ObjectSummary> summaries = AWSUtil.listS3Objects(bucketName, prefix);
                for( S3ObjectSummary summary : summaries ){
                    if( StringUtils.endsWith(summary.getKey(), "csv") ){
                        continue;
                    }
                    S3Object transcriptObject = s3Client.getObject(summary.getBucketName(), summary.getKey());
                    String original_transcript = IOUtils.toString(transcriptObject.getObjectContent());
                    int oringalLength = original_transcript.length();
//                    System.out.println("transcript " + transcript);
                    String transcript = fixPhoneme(original_transcript, fixes, phonemePattern);
                    transcript = fixPronunciation(transcript, fixes, wordPattern, isEnglish);
                    if( !StringUtils.equals(original_transcript, transcript) ){
                        System.out.println("summary.getBucketName(), summary.getKey() " + summary.getBucketName() + "," + summary.getKey());
//                        System.out.println(" transcript " + transcript);
                        File localFile = createLocalTranscript(transcript, summary.getBucketName(), summary.getKey());
                        List<Tag> tags = AWSUtil.getTags(summary.getBucketName(), summary.getKey());
                        AWSUtil.uploadS3Object(summary.getBucketName(), summary.getKey(), localFile, tags);
                    }

                }//end of for loop
            }//end of for loop
        }catch(IOException e){
            System.out.println(ExceptionUtils.getStackTrace(e));
        }

        return null;
    }

    private File createLocalTranscript(String transcript, String bucketName, String path) throws IOException {

        String completePath = "/tmp/"+bucketName+"/"+path;
        String parentPath = StringUtils.substring(completePath, 0, StringUtils.lastIndexOf(completePath, "/"));
        File parent = new File(parentPath);
        if( !parent.exists() ){
            parent.mkdirs();
        }

        System.out.println("complete path " + completePath);

        Path scriptPath = Path.of(completePath);
        Path filePath = Files.write(scriptPath, transcript.getBytes());

//        System.out.println("read again " + IOUtils.toString(new FileReader(filePath.toFile())));

        return filePath.toFile();

    }

    private String fixPronunciation(String transcript, Map<String, String> fixes, String to_be_found, boolean isEnglish) {
        String result = transcript;

        Pattern pattern = Pattern.compile(to_be_found);
        Matcher matcher = pattern.matcher(transcript);

        Map<String, String> beReplaced = new HashMap<>();
        while( matcher.find() ){
            String word = matcher.group();
            System.out.println("fixPronunciation word: " + word);

            beReplaced.put(word, fixes.get(word));
        }

        Set<Map.Entry<String, String>> entries = beReplaced.entrySet();
        for(Map.Entry<String, String> entry : entries){
            result = result.replaceAll(entry.getKey(), generatePronunciationReplacement(entry.getKey(), entry.getValue(), isEnglish));
        }

        return result;
    }

    private String generatePronunciationReplacement(String key, String value, boolean isEnglish) {

        if( isEnglish ){
            return "<phoneme alphabet=\"ipa\" ph=\""+value+"\">"+key+"</phoneme>";
        }
        return "<phoneme alphabet=\"sapi\" ph=\""+value+"\">"+key+"</phoneme>";
    }

    private String fixPhoneme(String transcript, Map<String, String> fixes, String to_be_found) {
        String result = transcript;

        Pattern pattern = Pattern.compile(to_be_found);
        Matcher matcher = pattern.matcher(transcript);

        Map<String, String> beReplaced = new HashMap<>();
        while( matcher.find() ){
            String ipa = matcher.group(1);
            String word = matcher.group(2);
            System.out.println("fixPhoneme ipa: " + ipa);
            System.out.println("fixPhoneme word: " + word);

            beReplaced.put(ipa, fixes.get(word));
        }

        Set<Map.Entry<String, String>> entries = beReplaced.entrySet();
        for(Map.Entry<String, String> entry : entries){
            result = result.replaceAll(entry.getKey(), entry.getValue());
        }

        return result;
    }

    private String genWordPattern(Map<String, String> fixes){
        if( fixes.isEmpty() ){
            return "";
        }
        //String regex = "(?<!<phoneme[^>]\\*>)\\b(Succoth|Etham)\\b(?!<\\/phoneme>)";
        StringBuilder builder = new StringBuilder("(?<!<phoneme[^>]\\*>)\\b(");
        Set<String> keys = fixes.keySet();
        for( String key : keys ){
            builder.append(StringUtils.trim(key)).append("|");
        }

        if( StringUtils.endsWith(builder, "|") ){
            builder = new StringBuilder(builder.substring(0, builder.toString().length()-1));
        }

        builder.append(")\\b(?!<\\/phoneme>)");

        return builder.toString();
    }

    private String genPhonemePattern(Map<String, String> fixes){
        if( fixes.isEmpty() ){
            return "";
        }
        //<phoneme\s+alphabet=\"ipa\"\s+ph=\"([^\"]+)\">(sin|george)</phoneme>
        StringBuilder builder = new StringBuilder("<phoneme\\s+alphabet=[\\\"|\\']ipa[\\\"|\\']\\s+ph=[\\\"|\\']([^\\\"|^\\']+)[\\\"|\\']>(");
        Set<String> keys = fixes.keySet();
        for( String key : keys ){
            builder.append(StringUtils.trim(key)).append("|");
        }

        if( StringUtils.endsWith(builder, "|") ){
            builder = new StringBuilder(builder.substring(0, builder.toString().length()-1));
        }

        builder.append(")</phoneme>");

        return builder.toString();
    }

    private Map<String, String> parseCSV(String csv) {
        Map<String, String> result = new HashMap<>();

        String[] lines = StringUtils.split(csv, System.lineSeparator());
        for( String line: lines ){
            String[] inputs = StringUtils.split(line, ",");
            result.put(StringUtils.trim(inputs[0]), StringUtils.trim(inputs[1]));
        }

        return result;
    }
}
