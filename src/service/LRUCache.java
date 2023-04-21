package src.service;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private final int capacity;
    private static BufferedWriter logWriterLRU;

    public LRUCache(int capacity) throws IOException {
        super(capacity, 0.75f, true);
        this.capacity = capacity;
        logWriterLRU = new BufferedWriter(new FileWriter("src/output/logs.txt"));
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        boolean removeEldest = size() > capacity;
        if (removeEldest) {
            K removedKey = eldest.getKey();
            String[] splitString = removedKey.toString().split("-");
            try {
                logWriterLRU.write("\nSWAP OUT F-" + splitString[0] + ", P-" + splitString[1]);
                logWriterLRU.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return removeEldest;
    }
}
