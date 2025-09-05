package com.arun.sample.util;

import com.arun.sample.model.ChunkDetail;
import software.amazon.awssdk.utils.Pair;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class S3Utility {

    private S3Utility(){}

    public static int detectLineTerminatorSize(InputStream input) throws IOException {
        int prev = -1, curr;
        while ((curr = input.read()) != -1) {
            if (curr == '\n') {
                return (prev == '\r') ? 2 : 1;
            }
            prev = curr;
        }
        return 1;
    }


    public static List<ChunkDetail> getChunkDetailList(Pair<Integer, Integer> lineSize, long totalBytes, int minLinesInChunk, int minChunkSize) {
        long newChunkSize = (long) lineSize.left() * minLinesInChunk;
        if(newChunkSize < minChunkSize) {
            newChunkSize = minChunkSize;
        }
        List<ChunkDetail> chunks = new ArrayList<>();
        long startingBytes = lineSize.right();
        int id =1;
        do {
            ChunkDetail state = new ChunkDetail();
            state.setId(id++);
            state.setStartPosition(startingBytes);
            state.setAvgLineSize(lineSize.left());
            state.setNewLineSize(lineSize.right());
            state.setTotalFileSize(totalBytes);
            startingBytes = startingBytes + newChunkSize;
            state.setEndPosition(Math.min(startingBytes, totalBytes));
            startingBytes++;
            chunks.add(state);
        } while(startingBytes < totalBytes);
        return chunks;
    }
}
