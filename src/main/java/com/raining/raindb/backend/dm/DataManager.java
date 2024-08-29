package com.raining.raindb.backend.dm;

import com.raining.raindb.backend.dm.dataItem.DataItem;
import com.raining.raindb.backend.dm.logger.Logger;
import com.raining.raindb.backend.dm.page.PageOne;
import com.raining.raindb.backend.dm.pageCache.PageCache;
import com.raining.raindb.backend.tm.TransactionManager;

/**
 * DataManager负责底层数据的管理和操作，
 * 为上层模块提供了方便的数据访问和操作接口，
 * 同时通过事务管理和日志记录等功能保证了数据的安全性和可靠性。
 *
 * >
 */
public interface DataManager {

    DataItem read(long uid) throws Exception;

    long insert(long xid, byte[] data) throws Exception;

    void close();

    //入口！！！
    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();
        return dm;
    }

    public static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }
        dm.fillPageIndex();
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);

        return dm;
    }
}
