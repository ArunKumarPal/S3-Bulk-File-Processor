package com.arun.sample.service;

import com.arun.sample.model.ChunkDetail;
import io.micronaut.context.annotation.Value;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.utils.Pair;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.arun.sample.constans.Constants.TAB_SEPARATOR;
import static com.arun.sample.util.S3Utility.detectLineTerminatorSize;
import static com.arun.sample.util.S3Utility.getChunkDetailList;

public class CSVFileProcessor {

    private final S3Service s3Service;

    @Value("${minChunkLines:5000}")
    private int minLinesInChunk;

    @Value("${minChunkSize:5242880}") // 5 * 1024 * 1024
    private int minChunkSize;

    public CSVFileProcessor(S3Service s3Service) {
        this.s3Service = s3Service;
    }

    public String[] getCsvHeaders(String bucketName, String key, String delimiter) {
        String headerLine = s3Service.getHeader(bucketName, key);
        String splitter = TAB_SEPARATOR.equals(delimiter) ? delimiter : "\\".concat(delimiter);
        return headerLine.trim().toLowerCase().split(splitter);
    }

    public String getUploadId(String bucketName, String outputFileKey){
        return s3Service.getUploadId(outputFileKey, bucketName);
    }

    public void completeFileUpload(String bucketName, String outputKey, String uploadId) {
        List<CompletedPart> parts = s3Service.listOfParts(outputKey, bucketName, uploadId).stream().map(part -> CompletedPart.builder().partNumber(part.partNumber()).eTag(part.eTag()).build()).toList();
        s3Service.completeMultiPartUpload(outputKey, bucketName, uploadId, parts);
    }

    public List<ChunkDetail> getFileChunks(String bucketName, String key) {
        long totalBytes = s3Service.getS3FileSize(bucketName, key);
        Pair<Integer, Integer> lineSize = estimateLineSize(100, key, bucketName);
        return getChunkDetailList(lineSize, totalBytes, minLinesInChunk, minChunkSize);
    }


    public Pair<Integer, Integer> estimateLineSize(int sampleLines, String key, String bucketName) {
        try (ResponseInputStream<GetObjectResponse> s3Object = s3Service.getS3Object(bucketName, key, 0, 1024 * 1024)) {
            int lineTerminatorSize = detectLineTerminatorSize(s3Object);
            BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object, StandardCharsets.UTF_8));
            String line;
            int count = 0;
            int totalSize = 0;
            while ((line = reader.readLine()) != null && count < sampleLines) {
                totalSize += line.getBytes(StandardCharsets.UTF_8).length + lineTerminatorSize; // +1 for newline
                count++;
            }
            return Pair.of(((totalSize + count)/ count), lineTerminatorSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int processChunkAndUploadInMultipart(ChunkDetail chunkDetail, String bucketName, String[] headers,  String fileInputObjectKey, String delimiter, String uploadId, String outputKey) {
        long adjustedStart = chunkDetail.getStartPosition() - chunkDetail.getNewLineSize();
        long adjustEnd = Math.min(chunkDetail.getEndPosition() + (2L * chunkDetail.getAvgLineSize()), chunkDetail.getTotalFileSize());
        AtomicInteger recordCount = new AtomicInteger(0);
        AtomicLong currentPosition = new AtomicLong(adjustedStart);
        s3Service.getInputStream(bucketName, fileInputObjectKey, adjustedStart, adjustEnd)
                .thenApply(inputStream -> Mono.using(
                        () -> new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8)),
                        reader -> processAddress(chunkDetail, delimiter, headers, currentPosition, recordCount, reader),
                        reader -> {
                            try {
                                inputStream.close();
                                reader.close();
                            } catch (IOException e) {
                               throw new RuntimeException(e);
                            }
                        }
                ).doOnNext(responseString -> s3Service.uploadFilePart(bucketName, outputKey, uploadId, chunkDetail.getId(), responseString.getBytes(StandardCharsets.UTF_8))).block()).join();
        return recordCount.get();
    }

    private Mono<String> processAddress(ChunkDetail chunkDetail, String delimiter, String[] headers, AtomicLong currentPosition, AtomicInteger recordCount, BufferedReader reader) {
        String splitter = TAB_SEPARATOR.equals(delimiter) ? delimiter : "\\".concat(delimiter);
        AtomicBoolean notCompleted = new AtomicBoolean(true);
        return Flux.defer(() -> Flux.fromStream(reader.lines()))
                .doOnNext(line -> currentPosition.set(currentPosition.get() + line.getBytes(StandardCharsets.UTF_8).length + chunkDetail.getNewLineSize()))
                .skip(1)
                .takeWhile(line -> currentPosition.get() < chunkDetail.getEndPosition() || notCompleted.getAndSet(false))
                .doOnNext(line -> recordCount.incrementAndGet())
             //   .map(line -> convertToRequestAddress(headers, line.split(splitter.concat(line_regex), -1), totalRecordCount))  // adding if required to convert csv line to any object
                .buffer(25)  // group the lines
          //      .flatMap(lines -> submitReqAndGetHeader(lines)
                .map(lines -> {
                    return String.join("\n", lines);
                })
                .collectList()
                .map(lines -> {
                    if(chunkDetail.getId() == 1){
                        String header = String.join(delimiter, headers); // change headers to your output header
                        return header + "\n" + String.join("\n", lines);
                    }
                    return String.join("\n", lines);
                });

    }

    // Todo : if require to convert row to java model
    public Object convertToModel(String[] header, String[] row){
        return null;
    }


    // Todo : if require to hit any external api to get the response of each lines and convert back to string
    private List<String> submitReqAndGetHeader(List<Object> requests) {
        return  null;
    }



}
