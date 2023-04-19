package src.service;

import src.dto.Operation;
import src.dto.Page;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataManager {
    private static LRUCache<String, Page> databaseBuffer;
    private static final Map<Integer, List<Record>> recoveryRecords = new HashMap<>();
    private static final Map<Integer, Integer> recordTracker = new HashMap<>();
    private static int numberOfPages;

    public static void executeOperation(Operation operation) {
        System.out.println("Operation received for transaction " + operation.getTransactionID() +
                " for operation " + operation.getOperationStr());
    }

    public static void initNumberOfPages(int pages) {
        numberOfPages = pages;
        databaseBuffer = new LRUCache<>(pages);
    }
}
