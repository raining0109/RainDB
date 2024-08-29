package com.raining.raindb.backend.im;

import com.raining.raindb.backend.common.SubArray;
import com.raining.raindb.backend.dm.dataItem.DataItem;
import com.raining.raindb.backend.tm.TransactionManagerImpl;
import com.raining.raindb.backend.util.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Node结构如下：
 * [LeafFlag][KeyNumber][SiblingUid]
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 */
public class Node {
    static final int IS_LEAF_OFFSET = 0;
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET+1;
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET+2;
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET+8;

    static final int BALANCE_NUMBER = 32;
    //2*8*(32*2+2)，乘以2，是因为填充因子为0.5,+2是空余1个，方便分裂
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2*8)*(BALANCE_NUMBER*2+2);

    BPlusTree tree;
    DataItem dataItem;
    SubArray raw;
    long uid;

    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if (isLeaf) {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 1;
        } else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 0;
        }
    }

    static boolean getRawIfLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte) 1;
    }

    static void setRawNoKeys(SubArray raw, int noKeys) {
        System.arraycopy(Parser.short2Byte((short)noKeys), 0, raw.raw, raw.start+NO_KEYS_OFFSET, 2);
    }

    static int getRawNoKeys(SubArray raw) {
        return (int)Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start+NO_KEYS_OFFSET, raw.start+NO_KEYS_OFFSET+2));
    }

    static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start+SIBLING_OFFSET, 8);
    }

    static long getRawSibling(SubArray raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start+SIBLING_OFFSET, raw.start+SIBLING_OFFSET+8));
    }

    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }

    static long getRawKthSon(SubArray raw, int kth) {
        //为什么是8*2，因为Son占八位，Key占8位
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }

    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(from.raw, offset, to.raw, to.start+NODE_HEADER_SIZE, from.end-offset);
    }

    //将从指定索引 kth 开始的元素向右移动两位
    static void shiftRawKth(SubArray raw, int kth) {
        int begin = raw.start+NODE_HEADER_SIZE+(kth+1)*(8*2);
        int end = raw.start+NODE_SIZE-1;
        for(int i = end; i >= begin; i --) {
            raw.raw[i] = raw.raw[i-(8*2)];
        }
    }

    /**
     * [LeafFlag: 0]  是否为叶子节点
     * [KeyNumber: 2]
     * [SiblingUid: 0]
     * [Son0: left][Key0: key][Son1: right][Key1: MAX_VALUE]
     *
     * 其中 LeafFlag 标记了该节点是否是个叶子节点；
     * KeyNumber 为该节点中 key 的个数；
     * SiblingUid 是其兄弟节点存储在 DM 中的 UID。
     * 后续是穿插的子节点（SonN）和 KeyN。
     * 最后的一个 KeyN 始终为 MAX_VALUE，以此方便查找。
     *
     * 该根节点的初始两个子节点为 left 和 right, 初始键值为 key
     * 注：一个简单的演示
     *
     *         (key)
     *        /     \
     *       /       \
     *      /         \
     *   [left]     [right]
     * @param left
     * @param right
     * @param key
     * @return
     */
    static byte[] newRootRaw(long left, long right, long key)  {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, false);
        setRawNoKeys(raw, 2);
        setRawSibling(raw, 0);
        setRawKthSon(raw, left, 0);
        setRawKthKey(raw, key, 0);
        setRawKthSon(raw, right, 1);
        setRawKthKey(raw, Long.MAX_VALUE, 1);

        return raw.raw;
    }

    /**
     * 生成一个空的根节点数据
     */
    static byte[] newNilRootRaw()  {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, true);
        setRawNoKeys(raw, 0);
        setRawSibling(raw, 0);

        return raw.raw;
    }

    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem di = bTree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    public void release() {
        dataItem.release();
    }

    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIfLeaf(raw);
        } finally {
            dataItem.rUnLock();
        }
    }

    //接下来要实现两个辅助搜索，在BPlusTree里面调用
    class  SearchNextRes {
        long uid;
        long siblingUid;
    }

    /**
     * 查找一个节点，线性扫描就足够了
     * searchNext 寻找对应 key 的 UID, 如果找不到, 则返回兄弟节点的 UID。
     * @param key
     * @return
     */
    public SearchNextRes searchNext(long key) {
        dataItem.rLock();
        try {
            SearchNextRes res = new SearchNextRes();
            int noKeys = getRawNoKeys(raw);
            for (int i = 0; i < noKeys; i++) {
                long ik = getRawKthKey(raw, i);
                if (key < ik) {
                    res.uid = getRawKthSon(raw, i);
                    res.siblingUid = 0;
                    return res;
                }
            }
            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    class LeafSearchRangeRes {
        List<Long> uids;
        long siblingUid;
    }

    /**
     * leafSearchRange 方法在当前节点进行范围查找，
     * 范围是 [leftKey, rightKey]，
     * 这里约定如果 rightKey 大于等于该节点的最大的 key,
     * 则还同时返回兄弟节点的 UID，方便继续搜索下一个节点。
     */
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();
        try {
            int noKeys = getRawNoKeys(raw);
            int kth = 0;
            while (kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik >= leftKey) {
                    break;
                }
                kth ++;
            }
            List<Long> uids = new ArrayList<>();
            while (kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik <= rightKey) {
                    uids.add(getRawKthSon(raw, kth));
                    kth ++;
                } else {
                    break;
                }
            }
            long siblingUid = 0;
            // 如果被遍历到了这个节点的最右边，获取兄弟节点的UID
            //意思就是 rightKey 大于等于该节点的最大的 key,
            if(kth == noKeys) {
                siblingUid = getRawSibling(raw);
            }

            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }


    class InsertAndSplitRes {
        long siblingUid, newSon, newKey;
    }

    /**
     * 在B+树的节点中插入一个键值对，并在需要时分裂节点
     * @param uid 插入的节点uid
     * @param key 待插入的键值对
     * @return
     * @throws Exception
     */
    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        //创建一个标志位，用于标记插入操作是否成功
        boolean success = false;
        // 创建一个异常对象，用于存储在插入或分裂节点时发生的异常
        Exception err = null;
        // 创建一个InsertAndSplitRes对象，用于存储插入和分裂节点的结果
        InsertAndSplitRes res = new InsertAndSplitRes();

        // 在数据项上设置一个保存点
        dataItem.before();
        try {
            // 尝试在节点中插入键值对，并获取插入结果
            success = insert(uid, key);
            // 如果插入失败，设置兄弟节点的UID，并返回结果
            if(!success) {
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            // 如果需要分裂节点
            if(needSplit()) {
                try {
                    // 分裂节点，并获取分裂结果
                    SplitRes r = split();
                    // 设置新节点的UID和新键，并返回结果
                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch(Exception e) {
                    // 如果在分裂节点时发生错误，保存异常并抛出
                    err = e;
                    throw e;
                }
            } else {
                // 如果不需要分裂节点，直接返回结果
                return res;
            }
        } finally {
            // 如果没有发生错误并且插入成功，提交数据项的修改
            if(err == null && success) {
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            } else {
                // 如果发生错误或插入失败，回滚数据项的修改
                dataItem.unBefore();
            }
        }
    }

    /**
     * 在B+树的节点中插入一个键值对的方法，不分裂
     * @param uid 插入的节点uid
     * @param key 待插入的键值对
     * @return 是否成功
     */
    private boolean insert(long uid, long key) {
        // 获取节点中的键的数量
        int noKeys = getRawNoKeys(raw);
        // 初始化插入位置的索引
        int kth = 0;
        // 找到第一个大于或等于要插入的键的键的位置
        while(kth < noKeys) {
            long ik = getRawKthKey(raw, kth);
            if(ik < key) {
                kth ++;
            } else {
                break;
            }
        }
        // 如果所有的键都被遍历过，并且存在兄弟节点，插入失败
        // 如果找不到，说明不应该插在这个节点，**应该**插在兄弟节点
        if(kth == noKeys && getRawSibling(raw) != 0) return false;

        // 如果节点是叶子节点
        if(getRawIfLeaf(raw)) {
            // 在插入位置后的所有键和子节点向后移动一位
            shiftRawKth(raw, kth);
            // 在插入位置插入新的键和子节点的UID
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
            // 更新节点中的键的数量
            setRawNoKeys(raw, noKeys+1);
        } else {
            // 如果节点是非叶子节点
            // 获取插入位置的键
            long kk = getRawKthKey(raw, kth);
            // 在插入位置插入新的键
            setRawKthKey(raw, key, kth);
            // 在插入位置后的所有键和子节点向后移动一位
            shiftRawKth(raw, kth+1);
            // 在插入位置的下一个位置插入原来的键和新的子节点的UID
            setRawKthKey(raw, kk, kth+1);
            setRawKthSon(raw, uid, kth+1);//重点在这里！！！
            // 更新节点中的键的数量
            setRawNoKeys(raw, noKeys+1);
        }
        return true;
    }

    private boolean needSplit() {
        return BALANCE_NUMBER*2 == getRawNoKeys(raw);
    }

    class SplitRes {
        long newSon, newKey;
    }

    /**
     * 分裂B+树的节点。
     * 当一个节点的键的数量达到 `BALANCE_NUMBER * 2` 时，就意味着这个节点已经满了，需要进行分裂操作。
     * 分裂操作的目的是将一个满的节点分裂成两个节点，每个节点包含一半的键。
     */
    private SplitRes split() throws Exception {
        // 创建一个新的字节数组，用于存储新节点的原始数据
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        // 设置新节点的叶子节点标志，与原节点相同
        setRawIsLeaf(nodeRaw, getRawIfLeaf(raw));
        // 设置新节点的键的数量为BALANCE_NUMBER
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);
        // 设置新节点的兄弟节点的UID，与原节点的兄弟节点的UID相同
        setRawSibling(nodeRaw, getRawSibling(raw));
        // 从原节点的原始字节数组中复制一部分数据到新节点的原始字节数组中
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);
        // 在数据管理器中插入新节点的原始数据，并获取新节点的UID
        long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);
        // 更新原节点的键的数量为 BALANCE_NUMBER
        setRawNoKeys(raw, BALANCE_NUMBER);
        // 更新原节点的兄弟节点的UID为新节点的UID
        setRawSibling(raw, son);

        // 创建一个SplitRes对象，用于存储分裂结果
        SplitRes res = new SplitRes();
        // 设置新节点的UID
        res.newSon = son;
        // 设置新键为新节点的第一个键的值
        //https://tuchuang-01.oss-cn-beijing.aliyuncs.com/img/20240506100057.png
        res.newKey = getRawKthKey(nodeRaw, 0);
        // 返回分裂结果
        return res; //向上传递的节点
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIfLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for(int i = 0; i < KeyNumber; i ++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }










}
