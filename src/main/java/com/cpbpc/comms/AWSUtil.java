package com.cpbpc.comms;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.util.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;

public class AWSUtil {

    private static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
    
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
            System.out.println("Object downloaded successfully to: " + localFilePath);
        } catch (Exception e) {
            e.printStackTrace();
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
            e.printStackTrace();
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
            e.printStackTrace();
        }
    }
}
