package com.arun.sample;

import com.arun.sample.model.ChunkDetail;
import com.arun.sample.service.CSVFileProcessor;
import io.micronaut.context.event.ApplicationEvent;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.runtime.Micronaut;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Application implements ApplicationEventListener<ApplicationEvent> {

    @Inject
    CSVFileProcessor fileProcessor;

    public static void main(String[] args) {
        Micronaut.run(Application.class, args);
    }

    @PostConstruct
    public void onPostConstruct() {
        String bucketName = System.getProperty("bucketName");
        String inputFileKey = System.getProperty("inputFileKey");
        String outputFileKey = System.getProperty("outputFileKey");
        String delimiter = System.getProperty("delimiter");
        int maxThread = System.getProperty("maxThread") != null ? Integer.parseInt(System.getProperty("maxThread")) : 10;
        ExecutorService executorService = Executors.newFixedThreadPool(maxThread);
        String[] csvHeaders = fileProcessor.getCsvHeaders(bucketName, inputFileKey, delimiter);
        List<ChunkDetail> chunkDetails = fileProcessor.getFileChunks(bucketName, inputFileKey);
        String uploadId = fileProcessor.getUploadId(bucketName, outputFileKey);
        List<CompletableFuture<Integer>> futures = chunkDetails.stream().map(chunkDetail -> CompletableFuture.supplyAsync(() -> fileProcessor.processChunkAndUploadInMultipart(chunkDetail, bucketName, csvHeaders, inputFileKey, delimiter, uploadId, outputFileKey), executorService)).toList();
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        int totalRecords = futures.stream().mapToInt(future -> {
            try {
                return future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }).sum();
        System.out.println("total records counts :- "+totalRecords);
        fileProcessor.completeFileUpload(bucketName, outputFileKey, uploadId);
        System.out.println("All processing done");
        executorService.close();
        System.exit(0);
    }

    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        // nothing
    }
}