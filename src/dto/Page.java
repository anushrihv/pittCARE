package src.dto;

import java.util.List;

public class Page {
    private int blockNumber;
    private List<SensorRecord> sensorRecords;

    public Page(int blockNumber, List<SensorRecord> sensorRecords) {
        this.blockNumber = blockNumber;
        this.sensorRecords = sensorRecords;
    }

    public int getBlockNumber() {
        return blockNumber;
    }

    public void setBlockNumber(int blockNumber) {
        this.blockNumber = blockNumber;
    }

    public List<SensorRecord> getRecords() {
        return sensorRecords;
    }

    public void setRecords(List<SensorRecord> sensorRecords) {
        this.sensorRecords = sensorRecords;
    }
}
