package com.raining.raindb.backend.common;

import com.raining.raindb.common.Error;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 实现一个基于引用计数策略的缓存框架
 */
public abstract class AbstractCache<T> {

    private HashMap<Long, T> cache;     //实际缓存的数据
    private HashMap<Long, Integer> references; //元素的引用个数
    private HashMap<Long, Boolean> getting; //正在获取某资源的线程

    private int maxResource; //缓存的最大缓存资源数
    private int count = 0; //缓存中元素的个数
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    //从缓存中获取资源
    protected T get(long key) throws Exception {
        //先看看有没有正在获取该资源的线程，有就一直等待
        while (true) {
            lock.lock();
            if (getting.containsKey(key)) {
                //请求的资源正在被其他线程获取
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            //现在，请求的资源没有被其他线程获取
            //先检查一下缓存
            if (cache.containsKey(key)) {
                //资源再缓存中
                T obj = cache.get(key);
                //增加资源对应的引用数量
                references.put(key, references.get(key) + 1);
                lock.unlock();
                //获取到了，直接返回
                return obj;
            }

            //缓存中没有，常听说获取该资源
            //先检查一下缓存有没有满
            if (maxResource > 0 && count == maxResource) {
                lock.unlock();
                //缓存满了
                throw Error.CacheFullException;
            }

            //现在缓存没有满，还没有
            count++;
            getting.put(key, true);
            lock.unlock();
            break;
        }

        T obj = null;
        try {
            obj = getForCache(key);
        } catch (Exception e) {
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        //lock的作用是保证getting，cache，references一致性
        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();

        return obj;
    }

    /**
     * 强行释放一个缓存
     *
     * @param key
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key) - 1;
            if (ref == 0) {
                //没有其他资源的引用了，可以释放了
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count--;
            } else {
                //否则，只是将引用数减一即可
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (Long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
//                references.remove(key);
//                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);
}
