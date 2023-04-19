package src.service;

import src.dto.LockType;
import src.dto.LockDetails;
import src.dto.Operation;

import java.util.*;
import java.util.concurrent.locks.Lock;

public class Scheduler {
    private static final HashMap<Integer, LockDetails> lockTable;
    private static final boolean[][] transactionCompatibilityTable;
    private static final HashMap<Integer, List<Operation>> transactionOperations;

    private static final char INSERT = 'I', RETRIEVE = 'R',
            COMMIT = 'C', ABORT = 'A', QUIT = 'Q', AGGREGATION = 'G', QUERY = 'M';

    static {
        lockTable = new HashMap<>();
        transactionCompatibilityTable = new boolean[2][2];
        transactionOperations = new HashMap<>();
        initializeCompatibilityTables();
    }

    private static void initializeCompatibilityTables() {
        transactionCompatibilityTable[LockType.READLOCK.getValue()][LockType.READLOCK.getValue()] = true;
    }

    /**
     * populate transaction map
     * add entry to lock table if it does not exist
     * add the transaction ID to the waitSet
     * resolve waitSet and extract operations ready to send to DM
     * send operation to data manager
     * @param operation
     */
    public static void scheduleOperation(Operation operation) {
        addTransactionOperation(operation);
        Integer sensorID = operation.getSensorID();
        if(sensorID != null) {
            // read or write operation
            createLockTableEntryIfNotExists(sensorID);
            lockTable.get(sensorID).getWaitingTransactions().add(operation.getTransactionID());
            Operation operationToSend = null;
            do {
                operationToSend = resolveWaitingTransaction(sensorID);
                if (operationToSend != null) {
                    lockTable.get(sensorID).getWaitingTransactions().remove(operationToSend.getTransactionID());
                    sendOperationToDataManager(operationToSend);
                }
            } while (operationToSend != null);
        }
    }

    public static void printLockTable() {
        for (int sensorID: lockTable.keySet()) {
            System.out.println(" ::::::::: Lock table for sensor ID " + sensorID + " ::::::::::");
            LockDetails lockDetails = lockTable.get(sensorID);
            System.out.println("Current lock type " + lockDetails.getLockType());
            System.out.println("Write lock owner " + lockDetails.getWriteLockOwner());
            System.out.println("Read lock owners " + lockDetails.getReadLockOwners());
            System.out.println("Waiting transactions " + lockDetails.getWaitingTransactions());
            System.out.println();
        }
    }

    private static void sendOperationToDataManager(Operation operationToSend) {
        DataManager.executeOperation(operationToSend);
    }

    /**
     * do all this in a loop
     * 1. if no transactions hold the lock, give lock to any waiting transaction
     * 2. if a transaction holds a lock (R or W), and new operations are available, schedule that operation
     * 2.1 if the new operation is a W and current lock is R, if it's the only read owner, upgrade the lock
     * 2.1 if the new operation is a commit, then release lock and call resolveWaitingTransaction
     * 3. find a transaction without conflicting operations and give it the lock
     * 4. if no such operations exist, then return
     */
    private static Operation resolveWaitingTransaction(int sensorID) {
        LockDetails lockDetails = lockTable.get(sensorID);
        Operation operationToSend = null;

        if(lockDetails.getLockType() == null ||
                (lockDetails.getReadLockOwners().isEmpty() && lockDetails.getWriteLockOwner() == null)) {
            // there are no transactions holding the lock
            Optional<Integer> waitingTransactionIfAny = lockDetails.getWaitingTransactions().stream().findAny();
            if (waitingTransactionIfAny.isPresent()) {
                // get any waiting transaction
                int waitingTransaction = waitingTransactionIfAny.get();
                operationToSend = getAndRemoveNextOperationToSchedule(waitingTransaction);
                transferLockOwnership(lockDetails, getLockTypeByOperation(operationToSend.getOperation()),
                        waitingTransaction);
            }
        } else {
            // if a transaction holds a lock (R or W), and new operations are available, schedule that operation
            operationToSend = findOperationToScheduleForExistingOwners(lockDetails);
            if (operationToSend != null && (operationToSend.isCommitOperation() || operationToSend.isAbortOperation())) {
                releaseLock(operationToSend.getTransactionID(), lockDetails);
            } else if (operationToSend != null) {
                return operationToSend;
            } else {
                // no new operations could be scheduled for existing owners
                // find new transactions without conflicting locks
                Integer waitingTransactionWithoutConflict = findWaitingTransactionWithNonConflictingOperations(lockDetails);
                if (waitingTransactionWithoutConflict != null) {
                    addWaitingTransactionAsLockOwner(waitingTransactionWithoutConflict, lockDetails);
                    operationToSend = getAndRemoveNextOperationToSchedule(waitingTransactionWithoutConflict);
                }
            }
        }

        return operationToSend;
//        if (operationToSend != null) {
//            // TODO send operation
//            // TODO check if operation is a commit
//            // see if any other operation can be resolved
//            resolveWaitingTransaction(sensorID);
//        } else {
//            // no transaction could be resolved in this cycle. Return.
//            return;
//        }
    }

    private static void releaseLock(int transactionID, LockDetails lockDetails) {
        if (lockDetails.getWriteLockOwner().equals(transactionID)) {
            // remove write lock owner
            lockDetails.setWriteLockOwner(null);
            if (lockDetails.getReadLockOwners().isEmpty()) {
                lockDetails.setLockType(null);
            } else {
                lockDetails.setLockType(LockType.READLOCK);
            }
        } else {
            // remove read lock owner
            lockDetails.getReadLockOwners().remove(transactionID);
            if (lockDetails.getReadLockOwners().isEmpty()) {
                lockDetails.setLockType(null);
            }
        }
    }

    /**
     * When the waiting transaction has no conflicts with the existing owners, add it as an owner
     */
    private static void addWaitingTransactionAsLockOwner(int waitingTransactionID, LockDetails lockDetails) {
        LockType lockRequested = findLockRequested(waitingTransactionID);
        if (lockRequested.equals(LockType.READLOCK)) {
            lockDetails.getReadLockOwners().add(waitingTransactionID);
        } else {
            lockDetails.setWriteLockOwner(waitingTransactionID);
        }
    }

    /**
     * If current lock is WRITELOCK, accept any operation
     * If current lock is READLOCK and new operation requires READLOCK, accept operation
     * If current lock is READLOCK and new operation requires WRITELOCK,
     *      accept operation if it's the sole owner of READLOCK
     */
    private static Operation findOperationToScheduleForExistingOwners(LockDetails lockDetails) {
        Integer writeLockOwner = lockDetails.getWriteLockOwner();
        Set<Integer> readLockOwners = lockDetails.getReadLockOwners();
        Operation operationToSend = null;

        if (writeLockOwner != null && transactionOperations.get(writeLockOwner).size() > 0) {
            operationToSend = getAndRemoveNextOperationToSchedule(writeLockOwner);
            return operationToSend;
        } else {
            for (int readLockOwner : readLockOwners) {
                if (transactionOperations.get(readLockOwner).size() > 0) {
                    operationToSend = getAndRemoveNextOperationToSchedule(readLockOwner);
                    if (operationToSend != null && operationToSend.isReadOperation()) {
                        return operationToSend;
                    } else if (operationToSend != null && operationToSend.isWriteOperation() && readLockOwners.size() == 1) {
                        // upgrade lock
                        lockDetails.setLockType(LockType.WRITELOCK);
                        lockDetails.getReadLockOwners().remove(readLockOwner);
                        lockDetails.setWriteLockOwner(readLockOwner);
                        return operationToSend;
                    }
                }
            }
        }

        return null;
    }

    private static Operation getAndRemoveNextOperationToSchedule(int transactionID) {
        if (transactionOperations.get(transactionID).size() > 0) {
            Operation nextOperation = transactionOperations.get(transactionID).get(0);
            transactionOperations.get(transactionID).remove(0);
            return nextOperation;
        }

        return null;
    }

    private static Integer findWaitingTransactionWithNonConflictingOperations(LockDetails lockDetails) {
        LockType currentLock = lockDetails.getLockType();
        Set<Integer> waitingTransactions = lockDetails.getWaitingTransactions();

        for (int waitingTransaction : waitingTransactions) {
            LockType lockRequested = findLockRequested(waitingTransaction);
            if (lockRequested != null && locksCompatible(lockRequested, currentLock)) {
                // locks do not conflict
                return waitingTransaction;
            } else if (lockRequested != null && lockRequested.equals(LockType.READLOCK) && isProcess(waitingTransaction)){
                // locks conflict, and lockRequested is a read lock and requesting transaction is a process
                return waitingTransaction;
            }
        }

        return null;
    }

    private static boolean isProcess(int transactionID) {
        return transactionOperations.get(transactionID).get(0).getIsolationLevel() == 0 ;
    }

    private static boolean locksCompatible(LockType requestedLock, LockType currentLock) {
        return transactionCompatibilityTable[requestedLock.getValue()][currentLock.getValue()];
    }

    private static LockType findLockRequested(int transactionID) {
        Operation firstOperation = transactionOperations.get(transactionID).get(0);
        if (firstOperation != null) {
            if (firstOperation.isReadOperation()) {
                return LockType.READLOCK;
            } else if (firstOperation.isWriteOperation()) {
                return LockType.WRITELOCK;
            }
        }

        return null;
    }

    /**
     * set the lock type and add the owner according to the lock requested
     */
    private static void transferLockOwnership(LockDetails lockDetails, LockType newLockType, int newTransactionID) {
        lockDetails.setLockType(newLockType);
        if (newLockType.equals(LockType.WRITELOCK)) {
            lockDetails.setWriteLockOwner(newTransactionID);
        } else {
            lockDetails.getReadLockOwners().add(newTransactionID);
        }
    }

    private static LockType getLockTypeByOperation(char operation) {
        if (operation == INSERT ) {
            return LockType.WRITELOCK;
        } else if (operation == RETRIEVE || operation == AGGREGATION || operation == QUERY){
            return LockType.READLOCK;
        } else {
            return null;
        }
    }

    private static void createLockTableEntryIfNotExists(int sensorID) {
        if (!lockTable.containsKey(sensorID)) {
            lockTable.put(sensorID, new LockDetails());
        }
    }

    private static void addTransactionOperation(Operation operation) {
        int transactionID = operation.getTransactionID();

        if (transactionOperations.containsKey(transactionID)) {
            transactionOperations.get(transactionID).add(operation);
        } else {
            List<Operation> operations = new ArrayList<>();
            operations.add(operation);
            transactionOperations.put(transactionID, operations);
        }
    }


}
