package src.dto;

public class TransactionDetails {
    public TransactionDetails() {
    }

    private int transactionID;
    private int lineNumberRead;
    private int isolationLevel;

    public int getTransactionID() {
        return transactionID;
    }

    public void setTransactionID(int transactionID) {
        this.transactionID = transactionID;
    }

    public int getLineNumberRead() {
        return lineNumberRead;
    }

    public void setLineNumberRead(int lineNumberRead) {
        this.lineNumberRead = lineNumberRead;
    }

    public int getIsolationLevel() {
        return isolationLevel;
    }

    public void setIsolationLevel(int isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

}
