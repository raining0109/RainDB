package com.raining.raindb.backend.vm;

import com.raining.raindb.backend.dm.DataManager;
import com.raining.raindb.backend.tm.TransactionManager;
import org.junit.Test;

public class VersionManagerTest {

    @Test
    public void testVM() {
        TransactionManager tm = TransactionManager.create("/home/rain/coding/RainDB/db_test/mydb");
        DataManager dm = DataManager.create("/home/rain/coding/RainDB/db_test/mydb", 100 * 8192, tm);
        VersionManager vm = VersionManager.newVersionManager(tm, dm);

        byte[] arr = {111};

        long xid = vm.begin(1);//开启一个事务，并拿到事务id：xid
        try {
            long uid = vm.insert(xid, arr);

            byte[] da = vm.read(xid, uid);
            System.out.println("read data: " + da);

            boolean succ = vm.delete(xid, uid);
            if (succ) {
                System.out.println("删除成功");
            } else {
                System.out.println("删除失败");
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
