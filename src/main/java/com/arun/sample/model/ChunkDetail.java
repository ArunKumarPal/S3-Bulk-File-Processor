package com.arun.sample.model;

import lombok.*;

@Data
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class ChunkDetail {
    private int id;
    private long startPosition;
    private long endPosition;
    private int newLineSize;
    private int avgLineSize;
    private boolean complete;
    private long totalFileSize;
}
