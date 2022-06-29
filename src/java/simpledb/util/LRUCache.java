package simpledb.util;

import java.util.*;

/**
 * @author freya
 * @date 2022/4/23
 **/
public class LRUCache<K,V> {
    public class LruNode{
        K key;
        V value;
        LruNode pre;
        LruNode next;

        public LruNode() {
        }

        public LruNode(K key, V value){
            this.key = key;
            this.value = value;
        }
    }

    private Map<K,LruNode> map;
    private int cap;
    private LruNode head, tail;

    public LRUCache(int cap) {
        this.cap = cap;
        this.head = new LruNode();
        this.tail = new LruNode();
        this.map = new HashMap<>();
        head.next = tail;
        tail.pre = head;
    }

    public int getSize() {
        return map.size();
    }

    public V get(K key){
        if (map.containsKey(key)){
            LruNode node = map.get(key);
            moveToHead(node);
            return node.value;
        }
        return null;
    }

    public void put(K key, V value){
        if (map.containsKey(key)){
            LruNode node = map.get(key);
            node.value = value;
            moveToHead(node);
        }else {
            LruNode node = new LruNode(key,value);
            map.put(key,node);
            addToHead(node);
        }
    }
    public V evictOldest(){
        if (this.getSize()<cap)return null;
        LruNode lruNode = removeTail();
        map.remove(lruNode.key);
        return lruNode.value;
    }

    public void remove(K key){
        if (map.containsKey(key)){
            removeNode(map.get(key));
            map.remove(key);
        }
    }

    private void addToHead(LruNode node){
        node.pre = head;
        node.next = head.next;
        head.next = node;
        node.next.pre = node;
    }

    private void removeNode(LruNode node){
        node.next.pre = node.pre;
        node.pre.next = node.next;
    }

    private void moveToHead(LruNode node){
        removeNode(node);
        addToHead(node);
    }

    private LruNode removeTail(){
        LruNode re = tail.pre;
        removeNode(re);
        return re;
    }

    public Iterator<V> getValueIterator(){
        List<V> list = new ArrayList<>();
        Set<Map.Entry<K, LruNode>> entries = map.entrySet();
        for (Map.Entry<K, LruNode> entry : entries) {
            list.add(entry.getValue().value);
        }
        return list.iterator();
    }

}
