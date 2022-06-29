package simpledb.transaction;

import simpledb.storage.PageId;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author freya
 * @date 2022/6/18
 **/
public class LockManager {

    ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, PageLock>> lockMap;

    public LockManager(){
        this.lockMap = new ConcurrentHashMap<>();
    }

    public synchronized boolean tryLock(PageId pid, TransactionId tid, int lockType, int timeout){
        long start = System.currentTimeMillis();
        while (true){
            if (System.currentTimeMillis() > start + timeout)return false;
            if (acquireLock(pid, tid, lockType))return true;
        }
    }

    public synchronized boolean acquireLock(PageId pid, TransactionId tid, int lockType){
        //该页没任何锁的数据
        //System.out.println("     -----------          " + pid);
        if(!this.lockMap.containsKey(pid)){
            PageLock lock = new PageLock(tid, lockType);
            ConcurrentHashMap<TransactionId, PageLock> map = new ConcurrentHashMap<>();
            lockMap.put(pid, map);
            map.put(tid, lock);
            return true;
        }
        ConcurrentHashMap<TransactionId, PageLock> locks = lockMap.get(pid);
        int hold = isHold(locks, tid);

        //事务对该页不持有锁
        if (hold == -1){
            //当前页没有锁
            if (locks.isEmpty()){
                locks.put(tid, new PageLock(tid, lockType));
                return true;
            }
            //有锁，则拿不到独占
            if (lockType == PageLock.EXCLUSIVE)return false;
            //有锁，且存在独占，也不行
            if (locks.size()==1)
                for (PageLock lock : locks.values()) {
                    if (lock.getType() == PageLock.EXCLUSIVE)return false;
                }
            return true;
        }

        //持有锁

        //持有共享锁
        if (hold == PageLock.SHARE){
            if (lockType == PageLock.SHARE)return true;

            //持有共享，并且只有一个事务持久，则可以升级
            if (locks.size() == 1){
                locks.get(tid).setType(PageLock.EXCLUSIVE);
                return true;
            }
            //升级失败
            return false;
        }

        //持有独占锁
        return true;
    }

    /**
     *
     * @param pid
     * @param tid
     * @return 不持有锁，返回-1， 否则返回对应的持有类型
     */
    public synchronized int isHold(PageId pid, TransactionId tid){
        if (!lockMap.containsKey(pid))return -1;
        ConcurrentHashMap<TransactionId, PageLock> locks = lockMap.get(pid);
        if (!locks.containsKey(tid))return -1;
        else return locks.get(tid).getType();
    }

    public synchronized int isHold(ConcurrentHashMap<TransactionId, PageLock> locks, TransactionId tid){
        if (!locks.containsKey(tid))return -1;
        else return locks.get(tid).getType();
    }

    public synchronized boolean releaseLock(PageId pid, TransactionId tid){
        if (isHold(pid,tid) == -1)return false;
        lockMap.get(pid).remove(tid);
        if (lockMap.get(pid).isEmpty())lockMap.remove(pid);
        return true;
    }

    public synchronized void releaseLockByTxn(TransactionId tid){
        for (PageId pageId : lockMap.keySet()) {
            if (isHold(pageId, tid)!=-1)releaseLock(pageId,tid);
        }
    }
}
