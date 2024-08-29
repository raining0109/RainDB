package com.raining.raindb.backend.common;

/**
 * 在 Java 中，当执行类似 subArray 的操作时，
 * 只会在底层进行一个复制，无法同一片内存。
 * 使用SubArray的方式暂时解决这个问题
 */
public class SubArray {
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
