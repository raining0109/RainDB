package com.raining.raindb.backend.dm;

import com.raining.raindb.backend.dm.dataItem.DataItem;
import com.raining.raindb.backend.tm.TransactionManager;
import com.raining.raindb.backend.tm.TransactionManagerImpl;
import com.raining.raindb.backend.util.Parser;
import org.junit.Test;

public class DataManagerTest {

    @Test
    public void testDM() {
        TransactionManager tm = TransactionManager.create("/home/rain/coding/RainDB/db_test/mydb");
        DataManager dm = DataManager.create("/home/rain/coding/RainDB/db_test/mydb", 100 * 8192, tm);

        byte[] data = Parser.string2Byte("HelloRainDB");
        System.out.println("data[] length:" + data.length + ":" + data);
        try {
            long uid = dm.insert(TransactionManagerImpl.SUPER_XID, data);
            dm.close();
            System.out.println(uid);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
