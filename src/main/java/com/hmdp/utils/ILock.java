package com.hmdp.utils;

/**
 * @author shkstart
 * @create 2023--14-20:53
 */
public interface ILock {

    /**
     * @Description:
     * 尝试获取锁
     * @return: boolean
     * @author: chen
     * @date: 2023/5/14 20:54
     */
    boolean tryLock(long timeoutSec);

    /**
     * @Description:
     * 释放锁
     * @return: void
     * @author: chen
     * @date: 2023/5/14 20:54
     */
    void unlock();
}
