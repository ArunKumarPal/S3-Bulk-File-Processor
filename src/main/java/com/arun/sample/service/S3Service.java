package com.arun.sample.service;

import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Singleton
public class S3Service {

    private static final Logger logger = LoggerFactory.getLogger(S3Service.class);

    private final S3AsyncClient s3AsyncClient;

    private final S3Client s3Client;

    public S3Service(S3AsyncClient s3AsyncClient, S3Client s3Client) {
        this.s3AsyncClient = s3AsyncClient;
        this.s3Client = s3Client;
    }

    public ResponseInputStream<GetObjectResponse> getS3Object(String bucket, String key, long start, int end){
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .range("bytes=" + start + "-" + end)
                .build();
        return s3Client.getObject(request);

    }

    public long getS3FileSize(String bucketName, String key) {
        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        HeadObjectResponse headObjectResponse = s3Client.headObject(headObjectRequest);
        return headObjectResponse.contentLength();
    }

    public String getHeader(String bucket, String key) {
        String headers;
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        // Read the first line of the CSV file
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(s3Client.getObject(request)))) {
            headers = reader.readLine();
        } catch (Exception e) {
            logger.error("Exception occurred while reading headers from S3 file: ", e);
            headers = "";
        }
        return headers;
    }

    public String getUploadId(String outputKey, String bucketName) {
        CreateMultipartUploadRequest createRequest = CreateMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(outputKey)
                .build();
        return s3Client.createMultipartUpload(createRequest).uploadId();
    }

    public List<Part> listOfParts(String outputKey, String bucketName, String uploadId){
        ListPartsRequest listPartsRequest = ListPartsRequest.builder()
                .bucket(bucketName)
                .key(outputKey)
                .uploadId(uploadId)
                .build();
        return s3Client.listParts(listPartsRequest).parts();
    }

    public void completeMultiPartUpload(String outputKey, String bucketName, String uploadId, List<CompletedPart> completedParts){
        CompleteMultipartUploadRequest request = CompleteMultipartUploadRequest.builder()
                .bucket(bucketName)
                .uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build()).key(outputKey)
                .build();
        s3Client.completeMultipartUpload(request);
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                        s3Client.getObject(GetObjectRequest.builder()
                                .bucket(bucketName)
                                .key(outputKey)
                                .build())
                ))) {

            long count = reader.lines().count();
            System.out.println("Output Line count: " + count);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<InputStream> getInputStream(String bucketName, String inputKey, long startPosition, long endPosition){
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(inputKey)
                .range("bytes=" + startPosition + "-" + endPosition)
                .build();
        return s3AsyncClient.getObject(request, AsyncResponseTransformer.toBlockingInputStream()).thenApply(responseInputStream -> {
            if (responseInputStream != null) {
                return responseInputStream;
            } else {
                throw new RuntimeException("Failed when reading from S3 bucket: " + bucketName + " with key: " + inputKey);
            } });

    }

    public void uploadFilePart(String bucketName, String key, String uploadId, int partNumber, byte[] bytes) {
        UploadPartRequest request = UploadPartRequest.builder()
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .build();
        RequestBody body = RequestBody.fromByteBuffer(ByteBuffer.wrap(bytes));
        UploadPartResponse uploadPartResponse =  s3Client.uploadPart(request, body);
        CompletedPart.builder()
                .eTag(uploadPartResponse.eTag())
                .partNumber(partNumber)
                .build();
    }
}
