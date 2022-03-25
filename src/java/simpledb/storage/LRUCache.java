package simpledb.storage;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class LRUCache<K,V> {
    class DLinkedNode {
        K key;
        V value;
        DLinkedNode prev;
        DLinkedNode next;
        public DLinkedNode() {}
        public DLinkedNode(K _key, V _value) {key = _key; value = _value;}

    }

    private Map<K, DLinkedNode> cache = new ConcurrentHashMap<K, DLinkedNode>();
    private int size;
    private int capacity;
    private DLinkedNode head, tail;

    public LRUCache(int capacity) {
        this.size = 0;
        this.capacity = capacity;
        // 使用伪头部和伪尾部节点
        head = new DLinkedNode();
        tail = new DLinkedNode();
        head.next = tail;
        head.prev = tail;
        tail.prev = head;
        tail.next = head;
    }


    public int getSize() {
        return size;
    }

    public DLinkedNode getHead() {
        return head;
    }

    public DLinkedNode getTail() {
        return tail;
    }

    public Map<K, DLinkedNode> getCache() {
        return cache;
    }
    //必须要加锁，不然多线程链表指针会成环无法结束循环。在这卡一天
    public synchronized V get(K key) {
        DLinkedNode node = cache.get(key);
        if (node == null) {
            return null;
        }
        // 如果 key 存在，先通过哈希表定位，再移到头部
        moveToHead(node);
        return node.value;
    }
    public synchronized void remove(DLinkedNode node){
        node.prev.next = node.next;
        node.next.prev = node.prev;
        cache.remove(node.key);
        size--;
    }

    public synchronized void discard(){
        // 如果超出容量，删除双向链表的尾部节点
        DLinkedNode tail = removeTail();
        // 删除哈希表中对应的项
        cache.remove(tail.key);
        size--;

    }
    public synchronized void put(K key, V value) {
        DLinkedNode node = cache.get(key);
        if (node == null) {
            // 如果 key 不存在，创建一个新的节点
            DLinkedNode newNode = new DLinkedNode(key, value);
            // 添加进哈希表
            cache.put(key, newNode);
            // 添加至双向链表的头部
            addToHead(newNode);
            ++size;
        }
        else {
            // 如果 key 存在，先通过哈希表定位，再修改 value，并移到头部
            node.value = value;
            moveToHead(node);
        }
    }

    private void addToHead(DLinkedNode node) {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    private void removeNode(DLinkedNode node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }


    private void moveToHead(DLinkedNode node) {
        removeNode(node);
        addToHead(node);
    }

    private DLinkedNode removeTail() {
        DLinkedNode res = tail.prev;
        removeNode(res);
        return res;
    }

}
