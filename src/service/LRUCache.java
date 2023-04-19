package src.service;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private final int capacity;

    public LRUCache(int capacity) {
        super(capacity, 0.75f, true);
        this.capacity = capacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        boolean removeEldest = size() > capacity;
        if (removeEldest) {
            K removedKey = eldest.getKey();
            // TODO WRITE LOG
        }
        return removeEldest;
    }
}
