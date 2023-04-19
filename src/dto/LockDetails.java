package src.dto;

import java.util.HashSet;
import java.util.Set;

public class LockDetails {
    private LockType lockType;
    private Set<Integer> readLockOwners;
    private Integer writeLockOwner;
    private Set<Integer> waitingTransactions;

    public LockDetails() {
        lockType = null;
        writeLockOwner = null;
        readLockOwners = new HashSet<>();
        waitingTransactions = new HashSet<>();
    }

    public LockType getLockType() {
        return lockType;
    }

    public void setLockType(LockType lockType) {
        this.lockType = lockType;
    }

    public Set<Integer> getReadLockOwners() {
        return readLockOwners;
    }

    public void setReadLockOwners(Set<Integer> readLockOwners) {
        this.readLockOwners = readLockOwners;
    }

    public Integer getWriteLockOwner() {
        return writeLockOwner;
    }

    public void setWriteLockOwner(Integer writeLockOwner) {
        this.writeLockOwner = writeLockOwner;
    }

    public Set<Integer> getWaitingTransactions() {
        return waitingTransactions;
    }

    public void setWaitingTransactions(Set<Integer> waitingTransactions) {
        this.waitingTransactions = waitingTransactions;
    }
}
