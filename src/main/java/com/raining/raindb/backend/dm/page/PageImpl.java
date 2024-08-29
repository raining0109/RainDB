package com.raining.raindb.backend.dm.page;

import com.raining.raindb.backend.dm.pageCache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * DM 将文件系统抽象成页面，每次对文件系统的读写都是以页面为单位的。
 * 同样，从文件系统读进来的数据也是以页面为单位进行缓存的。
 */
public class PageImpl implements Page{
    //这个页面的页号
    private int pageNumber;
    //这个页实际包含的字节数据
    private byte[] data;
    //这个页面是否是脏页面，在缓存驱逐的时候，脏页面需要被写回磁盘
    private boolean dirty;
    private Lock lock;

    //这里保存了一个 PageCache（还未定义）的引用，
    // 用来方便在拿到 Page 的引用时可以快速
    // 对这个页面的缓存进行释放操作。
    private PageCache pc;

    public PageImpl(int pageNumber, byte[] data, PageCache pc) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pc = pc;
        lock = new ReentrantLock();
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {
        pc.release(this);
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public int getPageNumber() {
        return pageNumber;
    }

    @Override
    public byte[] getData() {
        return data;
    }
}
