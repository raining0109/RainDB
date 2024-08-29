package com.raining.raindb.backend.vm;

import com.raining.raindb.backend.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * vm对于一个事务的抽象
 */
public class Transaction {
    // 事务的ID
    public long xid;
    // 事务的隔离级别
    public int level;
    // 事务的快照，用于存储活跃事务的ID
    public Map<Long, Boolean> snapshot;
    // 事务执行过程中的错误
    public Exception err;
    // 标志事务是否自动中止
    public boolean autoAborted;

    // 创建一个新的事务
    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        // 设置事务ID
        t.xid = xid;
        // 设置事务隔离级别
        t.level = level;
        // 如果隔离级别不为0，创建快照
        if (level != 0) {
            t.snapshot = new HashMap<>();
            // 将活跃事务的ID添加到快照中
            for (Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        // 返回新创建的事务
        return t;
    }

    // 判断一个事务ID是否在快照中
    public boolean isInSnapshot(long xid) {
        // 如果事务ID等于超级事务ID，返回false
        if (xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        // 否则，检查事务ID是否在快照中
        return snapshot.containsKey(xid);
    }
}
