package com.graeme;

public class MiniBatchInput {
    private String fileName;
    private int batchStart;
    private int batchEnd;


    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getBatchStart() {
        return batchStart;
    }

    public void setBatchStart(int batchStart) {
        this.batchStart = batchStart;
    }

    public int getBatchEnd() {
        return batchEnd;
    }

    public void setBatchEnd(int batchEnd) {
        this.batchEnd = batchEnd;
    }
}

