package com.raining.raindb.backend.dm.pageCache;

import com.raining.raindb.backend.common.AbstractCache;
import com.raining.raindb.backend.dm.page.Page;
import com.raining.raindb.backend.dm.page.PageImpl;
import com.raining.raindb.backend.util.Panic;
import com.raining.raindb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache{

    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;

    private AtomicInteger pageNumbers;

    public PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);//父类构造方法：初始化缓存
        //如果指定扽缓存容量太小，则直接报错
        if(maxResource < MEM_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int)length / PAGE_SIZE);
    }

    /**
     * 创建一个新的Page
     * 1. 自增获取page编号
     * 2. 创建PageImpl对象
     * 3. 写到磁盘
     * 4. 返回page编号
     * @param initData
     * @return
     */
    @Override
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet();
        Page pg = new PageImpl(pgno, initData, null);
        flush(pg);
        return pgno;
    }

    /**
     * 根据指定page编号获取页面，直接调用父方法，从缓存中找就行，
     * 找不到会自动调用getForCache方法去磁盘找
     * @param pgno
     * @return
     * @throws Exception
     */
    @Override
    public Page getPage(int pgno) throws Exception {
        return get((long)pgno);
    }

    /**
     * 根据pageNumber从数据库文件中读取页数据，并包裹成Page
     * @param key pageNumber
     * @return
     * @throws Exception
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int)key;
        long offset = pageOffset(pgno);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        //操作文件先加锁
        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        //文件操作完，记得解锁
        fileLock.unlock();
        //将字节数组包裹成Page对象
        return new PageImpl(pgno, buf.array(), this);
    }

    @Override
    protected void releaseForCache(Page pg) {
        if (pg.isDirty()) {
            flush(pg);
            pg.setDirty(false);
        }
    }

    @Override
    public void release(Page page) {
        release((long)page.getPageNumber());
    }

    /**
     * 这段代码实现了一个名为truncateByBgno的方法，
     * 其功能是根据给定的最大页号maxPgno截断（缩短）关联的文件，
     * 使其长度恰好足以容纳该页号对应的Page及其之前的所有Page。
     * @param maxPgno
     */
    @Override
    public void truncateByBgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void flushPage(Page pg) {
        flush(pg);
    }

    private void flush(Page pg) {
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);

        //操作文件的时候，加文件锁
        fileLock.lock();
        try {
            // 创建一个ByteBuffer对象，将Page对象中的数据包装进去。
            // ByteBuffer是Java NIO中用于高效处理字节数据的缓冲区，
            // 支持直接对内存进行读写，也能够与通道交互，实现数据的高效传输。
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fc.position(offset);
            fc.write(buf);
            //强制刷出所有已修改的数据到磁盘。
            // 参数为false表示不需要同步元数据（如文件的最后修改时间等）。
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }

    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    private static long pageOffset(int pgno) {
        return (long) (pgno - 1) * PAGE_SIZE;
    }
}
