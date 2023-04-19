package src.dto;

public class Record {
    private int sensorID;
    private int xLocation;
    private int yLocation;
    private long timestamp;
    private int heartRate;

    public Record() {
    }

    public Record(int sensorID, int xLocation, int yLocation, long timestamp, int heartRate) {
        this.sensorID = sensorID;
        this.xLocation = xLocation;
        this.yLocation = yLocation;
        this.timestamp = timestamp;
        this.heartRate = heartRate;
    }

    public int getSensorID() {
        return sensorID;
    }

    public void setSensorID(int sensorID) {
        this.sensorID = sensorID;
    }

    public int getxLocation() {
        return xLocation;
    }

    public void setxLocation(int xLocation) {
        this.xLocation = xLocation;
    }

    public int getyLocation() {
        return yLocation;
    }

    public void setyLocation(int yLocation) {
        this.yLocation = yLocation;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getHeartRate() {
        return heartRate;
    }

    public void setHeartRate(int heartRate) {
        this.heartRate = heartRate;
    }
}
