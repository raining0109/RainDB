package com.raining.raindb.backend.dm.pageIndex;

import com.raining.raindb.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 空闲页面的索引，
 * 方便根据所需页面大小直接获取相应的页面，而不需要遍历全部的页面
 */
public class PageIndex {
    //将一页划分成40个区间
    private static final int INTERVALS_NO = 40;
    //一个分区多大
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    private List<PageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        //空闲空间范围：0-1,1-2,...,39-40,>40
        lists = new List[INTERVALS_NO+1];
        for (int i = 0; i < INTERVALS_NO+1; i ++) {
            lists[i] = new ArrayList<>();
        }
    }

    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            int number = spaceSize / THRESHOLD;
            if(number < INTERVALS_NO) number ++;//下一个才有这么多空闲位置
            while(number <= INTERVALS_NO) {
                if(lists[number].size() == 0) {
                    number ++; //如果这个没有了，就找下一个，空闲空间只会更大
                    continue;
                }
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }
}
