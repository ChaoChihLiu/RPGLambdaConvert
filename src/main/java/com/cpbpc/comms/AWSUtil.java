package com.cpbpc.comms;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.internal.ExceptionUtils;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.util.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AWSUtil {

    private static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

    private static final DefaultAwsRegionProviderChain regionProvider = new DefaultAwsRegionProviderChain();

    private static AmazonS3 createS3Client (String accessPointAlias){

        AmazonS3 s3Client = null;
        if( StringUtils.isEmpty(accessPointAlias) ){
            s3Client = AmazonS3ClientBuilder.standard().build();
            return s3Client;
        }

        s3Client = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        "https://" + accessPointAlias + ".s3-accesspoint."+regionProvider+".amazonaws.com",
                        regionProvider.getRegion()))
                .build();

        return s3Client;
    }

    public static String searchS3ObjectKey(String bucketName, String prefix, String objectKeyPartial, String contentKeyword) throws IOException {
        System.out.println("bucket name: " + bucketName);
        System.out.println("prefix: " + prefix);
        System.out.println("objectKeyPartial: " + objectKeyPartial);
        System.out.println("contentKeyword: " + contentKeyword);

        // Create a request to list objects in the bucket
        ListObjectsV2Request request = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withPrefix(prefix);

        // List objects in the bucket
//        ListObjectsV2Result result = s3Client.listObjectsV2(request);
        ListObjectsV2Result result = null;
        String local_parent = "/tmp/audioMerge/";

        do {
            result = s3Client.listObjectsV2(request);

            for (S3ObjectSummary summary : result.getObjectSummaries()) {
                String objectKey = summary.getKey();
                if( !org.apache.commons.lang3.StringUtils.contains(objectKey, objectKeyPartial) ){
                    continue;
                }

                String local_file_path = local_parent+summary.getKey();
                File local_parent_folder = new File(StringUtils.substring(local_file_path, 0, StringUtils.lastIndexOf(local_file_path, "/")));
//            System.out.println( "local_parent_folder: " + local_parent_folder.getAbsolutePath() );
                if( !local_parent_folder.exists() ){
                    local_parent_folder.mkdirs();
                }
                downloadS3Object(summary.getBucketName(), summary.getKey(), local_file_path);
                String content = IOUtils.toString( new FileInputStream(new File(local_file_path)));
                if( !StringUtils.endsWith(content, ",") ){
                    content = content + ",";
                }
//            System.out.println("content: " + content);
//            System.out.println(" StringUtils.contains(content, contentKeyword): " + StringUtils.contains(content, contentKeyword));
                if(StringUtils.contains(content, contentKeyword) ){
                    System.out.println("Found matching object: " + objectKey);
                    return objectKey;
                }
            }

            String token = result.getNextContinuationToken();
            request.setContinuationToken(token);
        } while (result.isTruncated());
        return "";
    }

    public static String searchS3ObjectKey(String bucketName, String prefix, String objectKeyPartial){

        // Create a request to list objects in the bucket
        ListObjectsV2Request request = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withPrefix(prefix);

        // List objects in the bucket
        ListObjectsV2Result result = null;
        do {
            result = s3Client.listObjectsV2(request);

            // Iterate over the object summaries and search for the term
            for (S3ObjectSummary summary : result.getObjectSummaries()) {
                // Check if the object key contains the search term
                if (summary.getKey().contains(objectKeyPartial)) {
                    // Object key contains the search term, do something with it
                    System.out.println("Found matching object: " + summary.getKey());
                    return summary.getKey();
                }
            }

            String token = result.getNextContinuationToken();
            request.setContinuationToken(token);
        } while (result.isTruncated());


        return "";
    }
    
    public static void copyS3Objects(String bucketName, String outputPrefix, String outputFormat, String local_destination) {

        System.out.println("bucketName " + bucketName);
        System.out.println("outputPrefix " + outputPrefix);
        System.out.println("outputFormat " + outputFormat);
        System.out.println("local_destination " + local_destination);

        List<S3ObjectSummary> objects = listS3Objects(bucketName, outputPrefix);
        for( S3ObjectSummary object: objects ){
            String fileName = object.getKey();
            System.out.println("copyS3Objects object key " + fileName);
            if( !org.apache.commons.lang3.StringUtils.endsWith(fileName, outputFormat) ){
                continue;
            }

            downloadS3Object(bucketName, fileName, local_destination+fileName);
        }
    }

    public static void downloadS3Object(String bucketName, String objectKey, String localFilePath){

        try {
            File audioFile = new File(localFilePath);
            if( !audioFile.exists() ){
                audioFile.createNewFile();
            }

            S3Object s3Object = s3Client.getObject(bucketName, objectKey);

            FileOutputStream fos = new FileOutputStream(localFilePath);
            IOUtils.copy(s3Object.getObjectContent(), fos);
//            System.out.println("Object downloaded successfully to: " + localFilePath);
        } catch (Exception e) {
            System.out.println(ExceptionUtils.exceptionStackTrace(e));
        }
    }

    public static List<S3ObjectSummary> listS3Objects(String bucketName, String prefix){
        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request();
        listObjectsRequest.setBucketName(bucketName);
        listObjectsRequest.setPrefix(prefix);

        ListObjectsV2Result listObjectsResponse = s3Client.listObjectsV2(listObjectsRequest);
        List<S3ObjectSummary> objects = listObjectsResponse.getObjectSummaries();
        return objects;
    }

    public static void uploadS3Object(String bucketName, String objectKey, File localFile, List<Tag> tags){

        try {
            if( !localFile.exists() || !localFile.isFile() ){
                System.out.println(localFile.getAbsolutePath() + " not exist or not a file, skip");
                return;
            }

            PutObjectRequest request = new PutObjectRequest(bucketName, objectKey, localFile);
            request.setStorageClass(StorageClass.IntelligentTiering);
            request.setTagging(new ObjectTagging(tags));

            PutObjectResult result = s3Client.putObject(request);
            System.out.println("Object uploaded successfully to S3 bucket: " + localFile.getName());
        } catch (Exception e) {
            System.out.println(ExceptionUtils.exceptionStackTrace(e));
        }
    }

    public static void uploadS3Object(String bucketName, String prefix, String objectKey, File localFile, List<Tag> tags){

        try {
            if( !localFile.exists() || !localFile.isFile() ){
                System.out.println(localFile.getAbsolutePath() + " not exist or not a file, skip");
                return;
            }

            PutObjectRequest request = new PutObjectRequest(bucketName, prefix+objectKey, localFile);
            request.setStorageClass(StorageClass.IntelligentTiering);
            request.setTagging(new ObjectTagging(tags));

            PutObjectResult result = s3Client.putObject(request);
            System.out.println("Object uploaded successfully to S3 bucket: " + localFile.getName());
        } catch (Exception e) {
            System.out.println(ExceptionUtils.exceptionStackTrace(e));
        }
    }

    public static void uploadS3Object(String bucketName, String prefix, String objectKey, File localFile, List<Tag> tags, ObjectMetadata metadata){

        try {
            if( !localFile.exists() || !localFile.isFile() ){
                System.out.println(localFile.getAbsolutePath() + " not exist or not a file, skip");
                return;
            }

            PutObjectRequest request = new PutObjectRequest(bucketName, prefix+objectKey, localFile);
            request.setStorageClass(StorageClass.IntelligentTiering);
            request.setTagging(new ObjectTagging(tags));
            request.setMetadata(metadata);

            PutObjectResult result = s3Client.putObject(request);
            System.out.println("Object uploaded successfully to S3 bucket: " + localFile.getName());
        } catch (Exception e) {
            System.out.println(ExceptionUtils.exceptionStackTrace(e));
        }
    }

    public static List<Tag> getTags(String bucketName, String objectKey) {
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

        List<Tag> result = new ArrayList<>();
        Set<Map.Entry<String, String>> entries = tags.entrySet();
        for( Map.Entry entry : entries ){
            result.add(new Tag((String)entry.getKey(), (String)entry.getValue()));
        }

        return result;
    }

    public static Map<String, String> copyFromPreviousVersion(GetObjectTaggingResult currentVersion, String bucketName, String objectKey) {
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
}
