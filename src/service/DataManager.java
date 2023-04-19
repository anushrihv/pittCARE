package src.service;

import src.dto.Operation;

public class DataManager {
    public static void executeOperation(Operation operation) {
        System.out.println("Operation received for transaction " + operation.getTransactionID() +
                " for operation " + operation.getOperationStr());
    }
}
