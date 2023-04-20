package src.service;

import com.sun.source.tree.Tree;
import src.dto.Operation;
import src.dto.Page;
import src.dto.SensorRecord;

import java.io.BufferedWriter;
import java.util.*;

import java.io.FileWriter;
import java.io.IOException;
import src.dto.SensorRecord;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

public class DataManager {
    private static final int RECORD_SIZE = 24;
    private static final int NUMBER_OF_RECORDS_IN_BLOCK = 20;
    private static final int BLOCK_SIZE = RECORD_SIZE * NUMBER_OF_RECORDS_IN_BLOCK;
    private static LRUCache<String, Page> databaseBuffer;
    private static final Map<Integer, List<SensorRecord>> recoveryRecords = new HashMap<>();
    private static final Map<Integer, Integer> recordTracker = new HashMap<>();
    private static final String FILE_PREFIX = "src/output/";

    private static Map<Integer, TreeMap<Long, Integer>> timeStampIndex = new HashMap<>();
    private static Map<Integer, TreeMap<Integer, Integer>> xLocIndex = new HashMap<>();
    private static Map<Integer, TreeMap<Integer, Integer>> yLocIndex = new HashMap<>();
    private static Map<Integer, TreeMap<Integer, Integer>> heartRateIndex = new HashMap<>();


    private static BufferedWriter logWriter;

    public static void executeOperation(Operation operation) {
        System.out.println("Operation received for transaction " + operation.getTransactionID() +
                " for operation " + operation.getOperationStr());
        if (operation.isWriteOperation()) {
            SensorRecord sensorRecord = populateRecord(operation.getOperationStr());
            addRecoveryRecord(operation.getTransactionID(), sensorRecord);
        } else if (operation.isCommitOperation()) {
            performCommit(operation.getTransactionID());
            recoveryRecords.remove(operation.getTransactionID());
        } else if(operation.isAbortOperation()) {
            recoveryRecords.remove(operation.getTransactionID());
        }
    }

    private static void performCommit(int transactionID) {
        if (recoveryRecords.containsKey(transactionID)) {
            // get all records written by this transaction
            List<SensorRecord> sensorRecords = recoveryRecords.get(transactionID);
            for (SensorRecord sensorRecord : sensorRecords) {
                int blockNumber = getBlockNumber(sensorRecord.getSensorID());
                Page page = updateMemory(sensorRecord, blockNumber);
                updateDisk(page);
                updateIndices(sensorRecord);

                // increment the number of records written to the disk
                recordTracker.put(sensorRecord.getSensorID(),
                        recordTracker.getOrDefault(sensorRecord.getSensorID(), 0) + 1);
            }
        }
    }

    private static int getBlockNumber(int sensorID) {
        int totalNumberOfRecords = recordTracker.getOrDefault(sensorID, 0);
        return totalNumberOfRecords / NUMBER_OF_RECORDS_IN_BLOCK;
    }

    private static Page bringBlockToBufferIfNotExists(int sensorID, int blockNumber) {
        String key = sensorID + "-" + blockNumber;
        if(!databaseBuffer.containsKey(key)) {
            // read the corresponding block in the file
            int startOffset = blockNumber * BLOCK_SIZE;
            int endOffset = startOffset + BLOCK_SIZE;
            List<SensorRecord> sensorRecords = readSensorFile(getFilePath(sensorID), startOffset, endOffset);
            Page page = new Page(blockNumber, sensorRecords);
            databaseBuffer.put(key, page);
            return page;
        } else {
            return databaseBuffer.get(key);
        }

    }

    public static SensorRecord readSensorRecord(byte[] buffer, int offset) {
        SensorRecord record = new SensorRecord();
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, offset, RECORD_SIZE);
        record.setSensorID(byteBuffer.getInt());
        record.setxLocation(byteBuffer.getInt());
        record.setyLocation(byteBuffer.getInt());
        record.setTimestamp(byteBuffer.getLong());
        record.setHeartRate(byteBuffer.getInt());
        return record;
    }

    private static List<SensorRecord> readSensorFile(String filePath, int startOffset, int endOffset) {
        List<SensorRecord> sensorRecords = new ArrayList<>();
        try {
            FileInputStream inputStream = new FileInputStream(filePath);
            inputStream.skip(startOffset); // skip to the startOffset
            byte[] buffer = new byte[(int)(endOffset - startOffset)];
            inputStream.read(buffer); // read till endOffset - startOffset bytes

            int recordCount = buffer.length / RECORD_SIZE;
            for (int i = 0; i < recordCount; i++) {
                SensorRecord sensorRecord = readSensorRecord(buffer, i * RECORD_SIZE);
                if (sensorRecord.getSensorID() == 0) {
                    return sensorRecords;
                }
                sensorRecords.add(sensorRecord);
            }
        } catch (Exception ex) {
            System.out.println("Exception occurred while reading file " + ex.getMessage());
        }

        return sensorRecords;
    }

    private static Page updateMemory(SensorRecord sensorRecord, int blockNumber) {
        Page page = bringBlockToBufferIfNotExists(sensorRecord.getSensorID(), blockNumber);
        if (page.getRecords().size() < NUMBER_OF_RECORDS_IN_BLOCK) {
            // there is space in the existing page
            page.getRecords().add(sensorRecord);
        } else {
            // there is no space. Create a new page
            page = new Page(blockNumber, List.of(sensorRecord));
            databaseBuffer.put(getDatabaseBufferKey(sensorRecord.getSensorID(), blockNumber), page);
        }

        return page;
    }

    private static String getDatabaseBufferKey(int sensorID, int blockNumber) {
        return String.valueOf(sensorID) + '-' + blockNumber;
    }

    public static void writeSensorRecord(SensorRecord record, String fileName, int offset) {
        try {
            File file = new File(fileName);
            if (!file.exists()) {
                file.createNewFile();
            }

//            OutputStream outputStream = new FileOutputStream(fileName, false);
//            DataOutputStream dos = new DataOutputStream(outputStream);
//            dos.writeInt(record.getSensorID());
//            dos.writeInt(record.getxLocation());
//            dos.writeInt(record.getyLocation());
//            dos.writeLong(record.getTimestamp());
//            dos.writeInt(record.getHeartRate());
//            dos.flush();

            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            randomAccessFile.seek(offset);
            randomAccessFile.writeInt(record.getSensorID());
            randomAccessFile.writeInt(record.getxLocation());
            randomAccessFile.writeInt(record.getyLocation());
            randomAccessFile.writeLong(record.getTimestamp());
            randomAccessFile.writeInt(record.getHeartRate());
            randomAccessFile.close();
        }catch (Exception e){
            System.out.println("Failed to write sensor record " + e.getMessage());
        }
    }

    private static String getFilePath(int sensorID) {
        return FILE_PREFIX + sensorID + ".bin" ;
    }

    private static void updateDisk(Page page) {
        int startOffset = page.getBlockNumber() * BLOCK_SIZE;
        int numberOfRecords = page.getRecords().size();

        for (int i=0; i<numberOfRecords; i++) {
            SensorRecord sensorRecord = page.getRecords().get(i);
            writeSensorRecord(sensorRecord, getFilePath(sensorRecord.getSensorID()), startOffset);
            startOffset += RECORD_SIZE;
        }
    }

    private static void updateIndices(SensorRecord sensorRecord) {

    }

    private static SensorRecord populateRecord(String operationStr) {
        SensorRecord sensorRecord = new SensorRecord();
        String[] fields = operationStr.split(" ");

        sensorRecord.setSensorID(Integer.parseInt(fields[1]));
        sensorRecord.setxLocation(Integer.parseInt(fields[2]));
        sensorRecord.setyLocation(Integer.parseInt(fields[3]));
        sensorRecord.setTimestamp(Integer.parseInt(fields[4]));
        sensorRecord.setHeartRate(Integer.parseInt(fields[5]));

        return sensorRecord;
     }

     private static void addRecoveryRecord(int transactionID, SensorRecord sensorRecord) {
        if (recoveryRecords.containsKey(transactionID)) {
            recoveryRecords.get(transactionID).add(sensorRecord);
        } else {
            List<SensorRecord> sensorRecords = new ArrayList<>();
            sensorRecords.add(sensorRecord);
            recoveryRecords.put(transactionID, sensorRecords);
        }
     }

    public static void initNumberOfPages(int pages) {
        databaseBuffer = new LRUCache<>(pages);

        createLogFile();
    }

    public static void readAllRecords(int sensorID, int transactionID) throws IOException {
        int lastBlockInFile = recordTracker.get(sensorID) % 20;

        ArrayList<SensorRecord> allRecords = new ArrayList<>();
        for (int i = 0; i < lastBlockInFile; i++) {
            // TODO: bring the block to memory if it doesn't exist

            // Adding all records from this block to allRecords
            List<SensorRecord> recordsFromBlock = databaseBuffer.get(sensorID+"-"+i).getRecords();
            for (int j = 0; j < recordsFromBlock.size(); j++) {
                allRecords.add(recordsFromBlock.get(j));
            }
        }
        // Reading from recovery record for same transaction
        List<SensorRecord> extraRecords = recoveryRecords.get(transactionID);
        for (int k = 0; k < extraRecords.size(); k++) {
            allRecords.add(extraRecords.get(k));
        }
        // Writing all records to log
        for (int l = 0; l < allRecords.size(); l++) {
            logWriter.write("Read: ");
            printRecordToLog(allRecords.get(l));
        }
    }

    private static void printRecordToLog(SensorRecord r) throws IOException {
        logWriter.write(r.getSensorID() + ", " + r.getxLocation() + ", " + r.getyLocation() + ", " + r.getTimestamp() + ", " + r.getHeartRate());
        logWriter.flush();
    }

    private static void createLogFile() {
        try {
            logWriter = new BufferedWriter(new FileWriter("src/output/logs.txt", true));
            logWriter.write("Starting logging...");
            logWriter.flush();
            System.out.print("Should have written to file");
        } catch (IOException e) {
            System.out.println("An error occurred while writing to log file");
            e.printStackTrace();
        }
    }

    private static void M(String operation) throws IOException {
        // TODO format : M ID p val
        String[] splitString = operation.split(" ");

        if (splitString[2].equals("H")){
            heartRateM(Integer.parseInt(splitString[1]), Integer.parseInt(splitString[3]));
        } else if (splitString[2].equals("L")) {
            // TODO parse the array structure, extract numbers
            int xVal = Integer.parseInt(splitString[3].replaceAll("\\D", ""));
            int yVal = Integer.parseInt(splitString[4].replaceAll("\\D", ""));
            locationM(Integer.parseInt(splitString[1]), xVal, yVal);
        } else if (splitString[2].equals("T")) {
            // TODO parse the array structure, extract numbers
            long ts1 = Long.parseLong(splitString[3].replaceAll("\\D", ""));
            long ts2 = Long.parseLong(splitString[4].replaceAll("\\D", ""));
            timestampM(Integer.parseInt(splitString[1]), ts1, ts2);
        }
    }

    private static void heartRateM(int sensorID, int heartVal) throws IOException {
        TreeMap<Integer, Integer> heartRate = heartRateIndex.get(sensorID);
        int offset = heartRate.get(0);
        int blockID = offset / 480;
        // TODO bring into memory if not in buffer
        List<SensorRecord> recordsFromBlock = databaseBuffer.get(sensorID+"-"+blockID).getRecords();
        int entryIndex = (offset - (blockID * 480)) / 24;
        SensorRecord r = recordsFromBlock.get(entryIndex);
        logWriter.write("MRead: ");
        printRecordToLog(r);
    }

    private static void locationM(int sensorID, int xVal, int yVal) throws IOException {
        TreeMap<Integer, Integer> xLoc = xLocIndex.get(sensorID);
        TreeMap<Integer, Integer> yLoc = yLocIndex.get(sensorID);

        int x = xLoc.get(xVal);
        int y = yLoc.get(yVal);

        // code to get points 1 mile away from <x, y>
        for (int i = x - 1; i <= x + 1; i++) {
            for (int j = y - 1; j <= y + 1; j++) {
                if (i != x || j != y) { // exclude the given point
                    System.out.println("<" + i + "," + j + ">"); // print the pair of points
                    int currX = xLoc.get(i); // offset of x
                    int currY = yLoc.get(j); // offset of y

                    int blockID = currX / 480;
                    // TODO bring into memory if not in buffer

                    List<SensorRecord> recordsFromBlock = databaseBuffer.get(sensorID+"-"+blockID).getRecords();
                    int entryIndex = (currX - (blockID * 480)) / 24;
                    SensorRecord r = recordsFromBlock.get(entryIndex);
                    if (r.getyLocation() == currY) {
                        logWriter.write("MRead: ");
                        printRecordToLog(r);
                    }
                }
            }
        }
    }

    private static void timestampM(int sensorID, long ts1, long ts2) throws IOException {
        TreeMap<Long, Integer> timeStamp = timeStampIndex.get(sensorID);
        NavigableMap<Long, Integer> subMap = timeStamp.subMap(ts1, true, ts2, true);
        for (Map.Entry<Long, Integer> entry : subMap.entrySet()) {
            int blockID = entry.getValue() / 480;

            // TODO check if sensorID-blockID for that offset is in cache, if not, bring in cache


            List<SensorRecord> recordsFromBlock = databaseBuffer.get(sensorID+"-"+blockID).getRecords();
            int entryIndex = (entry.getValue() - (blockID * 480)) / 24;
            SensorRecord r = recordsFromBlock.get(entryIndex);
            logWriter.write("MRead: ");
            printRecordToLog(r);
        }
    }

    private static void G(String operation) throws IOException {
        // TODO format : G ID op p val
        String[] splitString = operation.split(" ");

        if (splitString[3].equals("T")) {
            long ts1 = Long.parseLong(splitString[4].replaceAll("\\D", ""));
            long ts2 = Long.parseLong(splitString[5].replaceAll("\\D", ""));
            logWriter.write("G: " + timestampG(Integer.parseInt(splitString[1]), splitString[2], ts1, ts2));
        } else if (splitString[3].equals("L")) {
            int xVal = Integer.parseInt(splitString[4].replaceAll("\\D", ""));
            int yVal = Integer.parseInt(splitString[5].replaceAll("\\D", ""));
            locationG(Integer.parseInt(splitString[1]), xVal, yVal);
        }
        logWriter.flush();
    }

    private static int timestampG(int sensorID, String op, long ts1, long ts2) {
        ArrayList<Integer> heartRateResult = new ArrayList<>();
        TreeMap<Long, Integer> timeStamp = timeStampIndex.get(sensorID);
        NavigableMap<Long, Integer> subMap = timeStamp.subMap(ts1, true, ts2, true);

        for (Map.Entry<Long, Integer> entry : subMap.entrySet()) {
            int blockID = entry.getValue() / 480;

            // TODO check if sensorID-blockID for that offset is in cache, if not, bring in cache


            List<SensorRecord> recordsFromBlock = databaseBuffer.get(sensorID+"-"+blockID).getRecords();
            int entryIndex = (entry.getValue() - (blockID * 480)) / 24;
            SensorRecord r = recordsFromBlock.get(entryIndex);
            int heart_val = r.getHeartRate();
            heartRateResult.add(heart_val);
        }
        return aggregationG(op, heartRateResult);
    }

    private static void locationG(int sensorID, int xVal, int yVal) {
        // TODO change return type to int
        ArrayList<Integer> heartRateResult = new ArrayList<>();
        TreeMap<Integer, Integer> xLoc = xLocIndex.get(sensorID);
        TreeMap<Integer, Integer> yLoc = yLocIndex.get(sensorID);
        NavigableMap<Integer, Integer> xSubMap = xLoc.subMap(xVal, true, yVal, true);
        NavigableMap<Integer, Integer> ySubMap = xLoc.subMap(xVal, true, yVal, true);
        // TODO figure out how to aggregate when only one point is given
    }

    private static int aggregationG(String op, ArrayList<Integer> heartRates) {
        int result = 0;
        if (op.equals("min")) {
            result = Collections.min(heartRates);
        } else if (op.equals("max")) {
            result = Collections.max(heartRates);
        } else if (op.equals("avg")) {
            result = (int)heartRates.stream().mapToDouble(num->num).average().orElse(0.0);
        }
        return result;
    }
}
