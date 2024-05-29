package com.cpbpc.rpgv2;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.VersionListing;
import com.cpbpc.comms.AWSUtil;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AutoMerger implements RequestHandler<S3Event, Void> {

    private static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

    @Override
    public Void handleRequest(S3Event s3event, Context context) {

        String prefix_regex = "/\\d{4}_\\d{2}/\\d{2}/";
        Pattern prefix_pattern = java.util.regex.Pattern.compile(prefix_regex);
        

        for (S3Event.S3EventNotificationRecord record : s3event.getRecords()) {
            String bucketName = URLDecoder.decode(record.getS3().getBucket().getName());
            String objectKey = URLDecoder.decode(record.getS3().getObject().getKey());

            System.out.println("bucket name: " + bucketName);
            System.out.println("object key: " + objectKey);

            Matcher matcher = prefix_pattern.matcher(objectKey);
            if( !matcher.find() ){
                 continue;
            }
            
            String audio_merge_bucket = System.getenv("audio_merge_bucket");
            String audio_merge_object_key = AWSUtil.searchS3ObjectKey(audio_merge_bucket,
                                                                        StringUtils.substring(objectKey, 0, StringUtils.lastIndexOf(objectKey, "/")+1),
                                                        "audioMerge") ;
            if( StringUtils.isEmpty(audio_merge_object_key) ){
                continue;
            }
            
            GetObjectTaggingRequest getObjectTaggingRequest = new GetObjectTaggingRequest(audio_merge_bucket, audio_merge_object_key);
            GetObjectTaggingResult getObjectTaggingResponse = s3Client.getObjectTagging(getObjectTaggingRequest);

            Map<String, String> tags = new HashMap<>();
            getObjectTaggingResponse.getTagSet().forEach(tag -> {
                tags.put(tag.getKey(), tag.getValue());
            });
            if( tags.isEmpty() ){
                tags.putAll(copyFromPreviousVersion( getObjectTaggingResponse, audio_merge_bucket, audio_merge_object_key ));
            }
            if (!verifyTags(List.of("audio_merged_prefix", "audio_merged_bucket", "audio_merged_format"), tags)) {
                continue;
            }

            File audio = mergeAudio(tags);
            List<Tag> tagList = new ArrayList<>();
            String date = tags.get("publish_date");
            String publisMonth = date.split("-")[0]+"_"+date.split("-")[1];
            String publisDate = date.split("-")[2];
            AWSUtil.uploadS3Object(tags.get("audio_merged_bucket"),
                    tags.get("audio_merged_prefix")+publisMonth+"/",
                    audio.getName(),
                    audio,
                    "audio/mpeg",
                    tagList);
        }

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

    public static File mergeAudio(Map<String, String> tags) {

        File local_audio_directory = new File("/tmp/audioMerge/");
        if( !local_audio_directory.exists() ){
            local_audio_directory.mkdirs();
        }

        String date = tags.get("publish_date");
        String publishMonth = date.split("-")[0] + "_" + date.split("-")[1];
        String publishDate = date.split("-")[2];

        File rpg_directory = new File( local_audio_directory.getAbsolutePath()+"/"+tags.get("output_prefix")+publishMonth+"/"+publishDate+"/" );
        if( !rpg_directory.exists() ){
            rpg_directory.mkdirs();
        }
        AWSUtil.copyS3Objects( tags.get("output_bucket"),
                tags.get("output_prefix")+publishMonth+"/"+publishDate+"/",
                tags.get("output_format"),
                local_audio_directory.getAbsolutePath()+"/");

        List<File> toBeMerged = new ArrayList<>();
        toBeMerged.addAll(List.of(rpg_directory.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if( StringUtils.endsWith(name, tags.get("output_format")) ){
                    return true;
                }
                return false;
            }
        })));
        toBeMerged.sort(new Comparator<File>() {
            @Override
            public int compare(File f1, File f2) {
                int i1 = Integer.parseInt(StringUtils.split(f1.getName(), "_")[0]);
                int i2 = Integer.parseInt(StringUtils.split(f2.getName(), "_")[0]);
                if( i1 < i2 ){
                    return -1;
                }
                if( i1 > i2 ){
                    return 1;
                }
                return 0;
            }
        });//end of list sort



        String finalName = tags.get("name_prefix")+StringUtils.remove(date, "-")+"."+tags.get("audio_merged_format");
        return mergeTo( toBeMerged, local_audio_directory.getAbsolutePath(), finalName );
    }


    public static File mergeTo(List<File> inputs, String filePath, String outputName ) {
        if(!StringUtils.endsWith(filePath, "/") ){
            filePath += "/";
        }

        File dir = new File(filePath);
        File result = new File(filePath + outputName);
        System.out.println("merge to " + filePath+outputName);
        try{
            if( !dir.exists() ){
                dir.mkdirs();
            }
            if(!result.exists()){
                result.createNewFile();
            }

            FileOutputStream sistream = new FileOutputStream(result);
            for( File file : inputs ){
                FileInputStream fistream = new FileInputStream(file);
                int temp;
                int size = 0;

                if( inputs.indexOf(file) > 0 ){
                    fistream.read(new byte[32],0,32);
                }

                temp = fistream.read();
                while( temp != -1){
                    sistream.write(temp);
                    temp = fistream.read();
                };
                fistream.close();
            }

        } catch (IOException e){
            e.printStackTrace();
        }

        return result;
    }

    private List<String> returnFiles(String input){
        List<String> list = new ArrayList<>();
        if( StringUtils.isEmpty(input) ){
            return list;
        }
        String[] temp = StringUtils.split(input, ",");
        list.addAll(List.of(temp));
        return list;
    }

    private boolean verifyTags(List<String> tagNames,Map<String, String> tags){

        for( String name : tagNames ){
            if( !tags.containsKey(name)
                    || StringUtils.isEmpty(tags.get(name)) ){
                System.out.println(name + " not exist or value is empty");
                return false;
            }
        }
        return true;
    }

}
