package com.raining.raindb.backend.vm;

import com.raining.raindb.backend.dm.DataManager;
import com.raining.raindb.backend.tm.TransactionManager;

/**
 * versionManager
 */
public interface VersionManager {

    byte[] read(long xid, long uid) throws Exception;

    long insert(long xid, byte[] data) throws Exception;

    boolean delete(long xid, long uid) throws Exception;

    long begin(int level);

    void commit(long xid) throws Exception;

    void abort(long xid);

    void close();

    public static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }
}
