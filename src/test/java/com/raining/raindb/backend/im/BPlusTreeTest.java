package com.raining.raindb.backend.im;

import java.io.File;
import java.util.List;

import com.raining.raindb.backend.dm.DataManager;
import com.raining.raindb.backend.dm.pageCache.PageCache;
import com.raining.raindb.backend.tm.TransactionManager;
import org.junit.Test;

public class BPlusTreeTest {
    @Test
    public void testTreeSingle() throws Exception {
        TransactionManager tm = TransactionManager.create("/home/rain/coding/RainDB/db_test/TestTreeSingle");
        DataManager dm = DataManager.create("/home/rain/coding/RainDB/db_test/TestTreeSingle", PageCache.PAGE_SIZE*10, tm);

        long root = BPlusTree.create(dm);
        BPlusTree tree = BPlusTree.load(root, dm);

        int lim = 10000;
        for(int i = lim-1; i >= 0; i --) {
            tree.insert(i, i);
        }

        for(int i = 0; i < lim; i ++) {
            List<Long> uids = tree.search(i);
            assert uids.size() == 1;
            assert uids.get(0) == i;
        }

        assert new File("/home/rain/coding/RainDB/db_test/TestTreeSingle.db").delete();
        assert new File("/home/rain/coding/RainDB/db_test/TestTreeSingle.log").delete();
        assert new File("/home/rain/coding/RainDB/db_test/TestTreeSingle.xid").delete();
    }
}
