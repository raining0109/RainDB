package com.raining.raindb.backend.vm;

import com.raining.raindb.backend.tm.TransactionManager;

/**
 * 可见性判断
 */
public class Visibility {

    /**
     *  判断通过事务t修改entry e 会不会产生版本跳跃
     * @param tm
     * @param t
     * @param e
     * @return
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if(t.level == 0) {
            //读已提交不会产生版本跳跃问题
            return false;
        } else {
            //e被xmax修改了，xmax被commit了，  xmax在t之后开始或者xmax在active列表中
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }


    // 用来在读提交的隔离级别下，某个记录是否对事务t可见
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        // 获取事务的ID
        long xid = t.xid;
        // 获取记录的创建版本号
        long xmin = e.getXmin();
        // 获取记录的删除版本号
        long xmax = e.getXmax();
        // 如果记录的创建版本号等于事务的ID并且记录未被删除，则返回true
        if (xmin == xid && xmax == 0) return true;

        // 如果记录的创建版本已经提交
        if (tm.isCommitted(xmin)) {
            // 如果记录未被删除，则返回true
            if (xmax == 0) return true;
            // 如果记录的删除版本号不等于事务的ID
            if (xmax != xid) {
                // 如果记录的删除版本未提交，则返回true
                // 因为没有提交，代表该数据还是上一个版本可见的
                if (!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }
        // 其他情况返回false
        return false;
    }

    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        //每个事务的active snap在事务启动时就初始化了，后续不会变化
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        //该版本是由当前事务创建，且未被删除
        if(xmin == xid && xmax == 0) return true;

        //忽略本事务开始时，还是active状态的事务的数据
        //该版本已经被提交，该版本在当前事务开启前创建，当前版本的事务不在active中
        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            //没有被删除，ok的
            if(xmax == 0) return true;
            //被其他事务删除了
            if(xmax != xid) {
                //这个其他事务没有提交，或者在本事务开启后开启，或者这个事务在active
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }
}
