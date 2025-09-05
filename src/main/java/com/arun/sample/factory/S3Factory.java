package com.arun.sample.factory;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

@Factory
public class S3Factory {
    @Bean
    @Singleton
    public S3AsyncClient s3AsyncClient() {
        return S3AsyncClient.builder()
                .build();
    }

    @Bean
    @Singleton
    public S3Client s3Client() {

        return S3Client.builder()
                .build();
    }



}
