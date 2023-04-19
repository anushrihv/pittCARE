package src;

import src.dto.Operation;
import src.service.Scheduler;

public class Main {
    public static void main(String[] args) {
        Operation operation = new Operation();
        operation.setTransactionID(1);
        operation.setOperationStr("R 13");
        operation.setIsolationLevel(1);
        Scheduler.scheduleOperation(operation);
    }
}
