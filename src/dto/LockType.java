package src.dto;

public enum LockType {
    READLOCK(0),
    WRITELOCK(1);

    private final int value;

    private LockType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
