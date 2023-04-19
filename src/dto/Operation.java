package src.dto;

public class Operation {

    public Operation() {
    }

    private int transactionID;
    private int isolationLevel;
    private String operationStr;

    public int getTransactionID() {
        return transactionID;
    }

    public void setTransactionID(int transactionID) {
        this.transactionID = transactionID;
    }

    public int getIsolationLevel() {
        return isolationLevel;
    }

    public void setIsolationLevel(int isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    public String getOperationStr() {
        return operationStr;
    }

    public void setOperationStr(String operationStr) {
        this.operationStr = operationStr;
    }

    public boolean isTransaction() {
        return isolationLevel == 1;
    }

    public boolean isProcess() {
        return isolationLevel == 0;
    }

    public boolean isReadOrWriteOperation() {
        char operation = getOperationStr().charAt(0);
        return operation == 'I' || operation == 'R' || operation == 'M' || operation == 'G';
    }

    public boolean isReadOperation() {
        char operation = getOperationStr().charAt(0);
        return operation == 'R' || operation == 'M' || operation == 'G';
    }

    public boolean isWriteOperation() {
        char operation = getOperationStr().charAt(0);
        return operation == 'I';
    }

    public boolean isCommitOperation() {
        char operation = getOperationStr().charAt(0);
        return operation == 'C';
    }

    public Integer getSensorID() {
        if (isReadOrWriteOperation()) {
            return Integer.valueOf(operationStr.split(" ")[1]);
        } else {
            return null;
        }
    }

    public char getOperation() {
        return operationStr.charAt(0);
    }
}
