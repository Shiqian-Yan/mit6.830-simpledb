package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    private Map<Integer, List<Lock>> map; //锁表

    public LockManager() {
        this.map = new ConcurrentHashMap<>();

    }

    public synchronized Boolean acquireLock(TransactionId tid, PageId pageId, Permissions permissions) {
        Integer pid = pageId.getPageNumber();
        Lock lock = new Lock(permissions, tid);
        List<Lock> locks = map.get(pid);
        if (locks == null) {
            locks = new ArrayList<>();
            locks.add(lock);
            map.put(pid, locks);
            return true;
        }
        if (locks.size() == 1) {  //只有一个事务占有锁
            Lock firstLock = locks.get(0);
            if (firstLock.getTransactionId().equals(tid)) {
                if (firstLock.getPermissions().equals(Permissions.READ_ONLY) && lock.getPermissions().equals(Permissions.READ_WRITE)) {
                    firstLock.setPermissions(Permissions.READ_WRITE); //锁升级
                }
                return true;
            } else {
                if (firstLock.getPermissions().equals(Permissions.READ_ONLY) && lock.getPermissions().equals(Permissions.READ_ONLY)) {
                    locks.add(lock);
                    return true;
                }
                return false;
            }
        }
        //list中有多个事务则说明全是共享锁
        if (lock.getPermissions().equals(Permissions.READ_WRITE)) {
            return false;
        }
        //同一个事务重复获取读锁，不要进入列表！
        for (Lock lock1 : locks) {
            if (lock1.getTransactionId().equals(tid)) {
                return true;
            }
        }
        locks.add(lock);
        return true;
    }


    public synchronized void releaseLock(TransactionId transactionId, PageId pageId) {
        List<Lock> locks = map.get(pageId.getPageNumber());
        for (int i = 0; i < locks.size(); i++) {
            Lock lock = locks.get(i);
            // release lock
            if (lock.getTransactionId().equals(transactionId)) {
                locks.remove(lock);
                if (locks.size() == 0) {
                    map.remove(pageId.getPageNumber());
                }
                return;
            }
        }
    }

    public synchronized void releaseAllLock(TransactionId transactionId) {
        for (Integer k : map.keySet()) {
            List<Lock> locks = map.get(k);
            for (int i = 0; i < locks.size(); i++) {
                Lock lock = locks.get(i);
                // release lock
                if (lock.getTransactionId().equals(transactionId)) {
                    locks.remove(lock);
                    if (locks.size() == 0) {
                        map.remove(k);
                    }
                    break;
                }
            }
        }
    }

    public synchronized Boolean holdsLock(TransactionId tid, PageId p) {
        List<Lock> locks = map.get(p.getPageNumber());
        for (int i = 0; i < locks.size(); i++) {
            Lock lock = locks.get(i);
            if (lock.getTransactionId().equals(tid)) {
                return true;
            }
        }
        return false;
    }
}
