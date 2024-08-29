package com.raining.raindb.backend.dm.logger;

import com.google.common.primitives.Bytes;
import com.raining.raindb.backend.util.Panic;
import com.raining.raindb.backend.util.Parser;
import com.raining.raindb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 日志文件读写
 *
 * 日志文件标准格式为：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum 为后续所有日志计算的Checksum，int类型
 *
 * 每条正确日志的格式为：
 * [Size] [Checksum] [Data]
 * Size 4字节int 标识Data长度
 * Checksum 4字节int
 */
public class LoggerImpl implements Logger{

    private static final int SEED = 13331;

    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;

    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;

    private long position;  // 当前日志指针的位置
    private long fileSize;  // 初始化时记录，log操作不更新
    private int xChecksum;

    /**
     * 不带xChecksum的构造方法
     * 用于打开一个已经存在的日志文件时调用
     * @param raf
     * @param fc
     */
    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    //带xChecksum的构造方法
    //用于创建一个新的日志文件时调用，初始化xChecksum为0
    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    /**
     * 初始化方法，用于打开一个日志文件时调用
     */
    void init() {
        //首先获取日志文件长度
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (size < 4) {
            Panic.panic(Error.BadLogFileException);
        }

        //读取checksum，checksum需要4个字节byte的空间
        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xChecksum;

        checkAndRemoveTail();
    }

    /**
     * 检查并移除bad tail
     * 注意：写log文件的时候，是先写log文件，再更新checksum
     */
    private void checkAndRemoveTail() {
        rewind();//将当前日志指针的位置设置为4

        //手动计算checkSum
        int xCheck = 0;
        while (true) {
            //不断地从文件中读取下一条日志，并将其中的 Data 解析出来并返回。
            byte[] log = internNext();
            if (log == null) break;
            xCheck = calChecksum(xCheck, log);
        }
        if(xCheck != xChecksum) {
            Panic.panic(Error.BadLogFileException);
        }

        //现在，通过检查了
        //将最后的badTail去除
        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        //truncate和seek的组合使用，是为了在执行完文件大小调整后，
        // 立即将操作焦点定位到特定位置，为后续的文件读写操作做好准备。
        try {
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        //将position恢复到4
        rewind();
    }

    /**
     * Logger 被实现成迭代器模式，通过 next() 方法，
     * 不断地从文件中读取下一条日志，并将其中的 Data 解析出来并返回。
     * next() 方法的实现主要依靠 internNext()，
     * 大致如下，其中 position 是当前日志文件读到的位置偏移：
     *  每条正确日志的格式为：
     *  [Size] [Checksum] [Data]
     *  Size 4字节int 标识Data长度
     *  Checksum 4字节int
     */
    private byte[] internNext() {
        //到头了
        if (position + OF_DATA >= fileSize) {
            return null;
        }
        //先解析size
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(tmp);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());
        if (position + size + OF_DATA > fileSize) {
            //data没来得及全部写进log
            return null;
        }
        //解析size+checksum+data，对data做一下求和，看看等不等于checksum
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        byte[] log = buf.array();
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        if(checkSum1 != checkSum2) {
            return null;
        }
        position += log.length;
        return log;
    }

    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buf);
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        updateXChecksum(log);
    }

    private void updateXChecksum(byte[] log) {
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        }
    }

    //传入data，组装日志格式：size+checksum+data
    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, checksum, data);
    }

    //截断
    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }


    @Override
    public void rewind() {
        position = 4;
    }

    //获取下一个日志的data部分
    //根据position指定的，position不断变化，一直next
    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if(log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch(IOException e) {
            Panic.panic(e);
        }
    }
}
