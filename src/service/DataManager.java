package src.service;

import com.sun.source.tree.Tree;
import src.dto.Operation;
import src.dto.Page;

import java.io.BufferedWriter;
import java.lang.reflect.Array;
import java.util.*;

import java.io.FileWriter;
import java.io.IOException;

import src.dto.SensorRecord;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

public class DataManager {
    private static final int RECORD_SIZE = 24;
    private static final int NUMBER_OF_RECORDS_IN_BLOCK = 3;
    private static final int BLOCK_SIZE = RECORD_SIZE * NUMBER_OF_RECORDS_IN_BLOCK;
    public static LRUCache<String, Page> databaseBuffer;
    private static final Map<Integer, List<SensorRecord>> recoveryRecords = new HashMap<>();
    private static final Map<Integer, Integer> recordTracker = new HashMap<>();
    private static final String FILE_PREFIX = "src/output/";

    private static Map<Integer, TreeMap<Long, ArrayList<Integer>>> timeStampIndex = new HashMap<>();
    private static Map<Integer, TreeMap<Integer, ArrayList<Integer>>> xLocIndex = new HashMap<>();
    private static Map<Integer, TreeMap<Integer, ArrayList<Integer>>> yLocIndex = new HashMap<>();
    public static Map<Integer, TreeMap<Integer, ArrayList<Integer>>> heartRateIndex = new HashMap<>();

    private static TreeMap<Long, ArrayList<Integer>> timeStampTree = new TreeMap<>();
    private static TreeMap<Integer, ArrayList<Integer>> xLocTree = new TreeMap<>();
    private static TreeMap<Integer, ArrayList<Integer>> yLocTree = new TreeMap<>();
    public static TreeMap<Integer, ArrayList<Integer>> heartRateTree = new TreeMap<>();

    private static BufferedWriter logWriter;

    public static void executeOperation(Operation operation) throws IOException {
        System.out.println("Operation received for transaction " + operation.getTransactionID() +
                " for operation " + operation.getOperationStr());
        if (operation.isWriteOperation()) {
            SensorRecord sensorRecord = populateRecord(operation.getOperationStr());
            addRecoveryRecord(operation.getTransactionID(), sensorRecord);
        } else if (operation.isCommitOperation()) {
            performCommit(operation.getTransactionID());
            recoveryRecords.remove(operation.getTransactionID());
        } else if (operation.isAbortOperation()) {
            recoveryRecords.remove(operation.getTransactionID());
        } else if (operation.isReadOperation()) {
            readAllRecords(operation.getOperationStr(), operation.getTransactionID());
        } else if (operation.isMOperation()) {
            M(operation.getOperationStr());
        } else if (operation.isGOperation()) {
            G(operation.getOperationStr());
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
                // increment the number of records written to the disk
                recordTracker.put(sensorRecord.getSensorID(),
                        recordTracker.getOrDefault(sensorRecord.getSensorID(), 0) + 1);
                updateIndices(sensorRecord);

                //System.out.print(recordTracker.get(sensorRecord.getSensorID()));
            }
        }
    }

    private static int getBlockNumber(int sensorID) {
        int totalNumberOfRecords = recordTracker.getOrDefault(sensorID, 0);
        return totalNumberOfRecords / NUMBER_OF_RECORDS_IN_BLOCK;
    }

    private static Page bringBlockToBufferIfNotExists(int sensorID, int blockNumber) {
        String key = sensorID + "-" + blockNumber;
        if (!databaseBuffer.containsKey(key)) {
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
            byte[] buffer = new byte[(int) (endOffset - startOffset)];
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
            ex.printStackTrace();
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
            List<SensorRecord> newList = new ArrayList<>();
            newList.add(sensorRecord);
            page = new Page(blockNumber + 1, newList);
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
        } catch (Exception e) {
            System.out.println("Failed to write sensor record " + e.getMessage());
        }
    }

    private static String getFilePath(int sensorID) {
        return FILE_PREFIX + sensorID + ".bin";
    }

    private static void updateDisk(Page page) {
        int startOffset = page.getBlockNumber() * BLOCK_SIZE;
        int numberOfRecords = page.getRecords().size();

        for (int i = 0; i < numberOfRecords; i++) {
            SensorRecord sensorRecord = page.getRecords().get(i);
            writeSensorRecord(sensorRecord, getFilePath(sensorRecord.getSensorID()), startOffset);
            startOffset += RECORD_SIZE;
        }
    }

    private static void updateIndices(SensorRecord sensorRecord) {
        int offset = (recordTracker.get(sensorRecord.getSensorID()) - 1) * RECORD_SIZE;

        // TimeStamp Index:
        ArrayList<Integer> offsets = timeStampTree.get(sensorRecord.getTimestamp());
        if (offsets == null) {
            offsets = new ArrayList<>();
        }
        offsets.add(offset);
        timeStampTree.put(sensorRecord.getTimestamp(), offsets);
        timeStampIndex.put(sensorRecord.getSensorID(), timeStampTree);

        // X Location Index:
        offsets = xLocTree.get(sensorRecord.getxLocation());
        if (offsets == null) {
            offsets = new ArrayList<>();
        }
        offsets.add(offset);
        xLocTree.put(sensorRecord.getxLocation(), offsets);
        xLocIndex.put(sensorRecord.getSensorID(), xLocTree);

        // Y Location Index:
        offsets = yLocTree.get(sensorRecord.getyLocation());
        if (offsets == null) {
            offsets = new ArrayList<>();
        }
        offsets.add(offset);
        yLocTree.put(sensorRecord.getyLocation(), offsets);
        yLocIndex.put(sensorRecord.getSensorID(), yLocTree);

        // Heart Rate Index:
        offsets = heartRateTree.get(sensorRecord.getHeartRate());
        if (offsets == null) {
            offsets = new ArrayList<>();
        }
        offsets.add(offset);
        heartRateTree.put(sensorRecord.getHeartRate(), offsets);
        heartRateIndex.put(sensorRecord.getSensorID(), heartRateTree);
    }

    private static SensorRecord populateRecord(String operationStr) {
        SensorRecord sensorRecord = new SensorRecord();
        String[] fields = operationStr.split(" ");

        sensorRecord.setSensorID(Integer.parseInt(fields[1].replaceAll("\\D", "")));
        sensorRecord.setxLocation(Integer.parseInt(fields[2].replaceAll("\\D", "")));
        sensorRecord.setyLocation(Integer.parseInt(fields[3].replaceAll("\\D", "")));
        sensorRecord.setTimestamp(Integer.parseInt(fields[4].replaceAll("\\D", "")));
        sensorRecord.setHeartRate(Integer.parseInt(fields[5].replaceAll("\\D", "")));

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

    public static void readAllRecords(String operation, int transactionID) throws IOException {
        String[] splitString = operation.split(" ");
        int sensorID = Integer.parseInt(splitString[1]);

        int lastBlockInFile = 0;
        if (!recordTracker.isEmpty()) {
            lastBlockInFile = recordTracker.get(sensorID) % NUMBER_OF_RECORDS_IN_BLOCK;
        }

        ArrayList<SensorRecord> allRecords = new ArrayList<>();
        for (int i = 0; i < lastBlockInFile; i++) {
            bringBlockToBufferIfNotExists(sensorID, i);

            // Adding all records from this block to allRecords
            List<SensorRecord> recordsFromBlock = databaseBuffer.get(getDatabaseBufferKey(sensorID, i)).getRecords();
            for (int j = 0; j < recordsFromBlock.size(); j++) {
                allRecords.add(recordsFromBlock.get(j));
            }
        }
        // Reading from recovery record for same transaction
        List<SensorRecord> extraRecords = recoveryRecords.get(transactionID);
        if (extraRecords != null) {
            for (int k = 0; k < extraRecords.size(); k++) {
                allRecords.add(extraRecords.get(k));
            }
        }
        // Writing all records to log
        for (int l = 0; l < allRecords.size(); l++) {
            logWriter.write("\nRead: ");
            printRecordToLog(allRecords.get(l));
        }
    }

    private static void printRecordToLog(SensorRecord r) throws IOException {
        logWriter.write(r.getSensorID() + ", " + r.getxLocation() + ", " + r.getyLocation() + ", " + r.getTimestamp() + ", " + r.getHeartRate());
        logWriter.flush();
    }

    private static void createLogFile() {
        try {
            logWriter = new BufferedWriter(new FileWriter(FILE_PREFIX + "logs.txt", true));
            logWriter.write("Starting logging...");
            logWriter.flush();
            System.out.print("Written to file");
        } catch (IOException e) {
            System.out.println("An error occurred while writing to log file");
            e.printStackTrace();
        }
    }

    private static void M(String operation) throws IOException {
        String[] splitString = operation.split(" ");

        if (splitString[2].equals("H")) {
            heartRateM(Integer.parseInt(splitString[1]), Integer.parseInt(splitString[3]));
        } else if (splitString[2].equals("L")) {
            int xVal = Integer.parseInt(splitString[3].replaceAll("\\D", ""));
            int yVal = Integer.parseInt(splitString[4].replaceAll("\\D", ""));
            locationMM(Integer.parseInt(splitString[1]), xVal, yVal);
        } else if (splitString[2].equals("T")) {
            long ts1 = Long.parseLong(splitString[3].replaceAll("\\D", ""));
            long ts2 = Long.parseLong(splitString[4].replaceAll("\\D", ""));
            timestampM(Integer.parseInt(splitString[1]), ts1, ts2);
        }
    }

    public static void heartRateM(int sensorID, int heartVal) throws IOException {
        TreeMap<Integer, ArrayList<Integer>> heartRate = heartRateIndex.get(sensorID);
        ArrayList<Integer> heartRateOffsets = heartRate.get(heartVal);

        for (Integer offset : heartRateOffsets) {
            int blockID = offset / BLOCK_SIZE;
            bringBlockToBufferIfNotExists(sensorID, blockID);
            List<SensorRecord> recordsFromBlock = databaseBuffer.get(getDatabaseBufferKey(sensorID, blockID)).getRecords();
            int entryIndex = (offset - (blockID * BLOCK_SIZE)) / RECORD_SIZE;
            SensorRecord r = recordsFromBlock.get(entryIndex);

            logWriter.write("\nMRead: ");
            printRecordToLog(r);
        }

    }

    private static void locationMM(int sensorID, int xVal, int yVal) throws IOException {
        Set<Integer> xValues = new HashSet<>();
        Set<Integer> yValues = new HashSet<>();

        xValues.addAll(xLocIndex.get(sensorID).getOrDefault(xVal - 1, new ArrayList<>()));
        xValues.addAll(xLocIndex.get(sensorID).getOrDefault(xVal, new ArrayList<>()));
        xValues.addAll(xLocIndex.get(sensorID).getOrDefault(xVal + 1, new ArrayList<>()));
        yValues.addAll(yLocIndex.get(sensorID).getOrDefault(yVal - 1, new ArrayList<>()));
        yValues.addAll(yLocIndex.get(sensorID).getOrDefault(yVal, new ArrayList<>()));
        yValues.addAll(yLocIndex.get(sensorID).getOrDefault(yVal + 1, new ArrayList<>()));

        xValues.retainAll(yValues);

        for (Integer offset : xValues) {
            int blockID = offset / BLOCK_SIZE;
            bringBlockToBufferIfNotExists(sensorID, blockID);
            List<SensorRecord> recordsFromBlock = databaseBuffer.get(getDatabaseBufferKey(sensorID, blockID)).getRecords();
            int entryIndex = (offset - (blockID * BLOCK_SIZE)) / RECORD_SIZE;
            SensorRecord r = recordsFromBlock.get(entryIndex);

            logWriter.write("\nLocation including given MRead: ");
            printRecordToLog(r);
            System.out.println(offset);
        }
    }

    private static void locationM(int sensorID, int xVal, int yVal) throws IOException {
        TreeMap<Integer, ArrayList<Integer>> xLoc = xLocIndex.get(sensorID);
        TreeMap<Integer, ArrayList<Integer>> yLoc = yLocIndex.get(sensorID);

        // code to get points 1 mile away from <x, y>
        for (int i = xVal - 1; i <= xVal + 1; i++) {
            for (int j = yVal - 1; j <= yVal + 1; j++) {
                if (i == xVal && j == yVal) {
                    continue;
                }// exclude the given point

                System.out.println("<" + i + "," + j + ">"); // print the pair of points

                ArrayList<Integer> xOffsets = xLoc.get(i); // offsets of x
                ArrayList<Integer> yOffsets = yLoc.get(j); // offsets of y

                if (xOffsets != null && yOffsets != null) {
                    System.out.println("entered block for " + i + ", " + j);

                    System.out.println("xoffsets" + xOffsets);
                    System.out.println("yoffsets" + yOffsets);

                    xOffsets.retainAll(yOffsets);
                    System.out.println("Size of xOffsets after retainAll: " + xOffsets.size());
                    for (Integer offset : xOffsets) {
                        System.out.println("entered second block for " + i + ", " + j);
                        int blockID = offset / BLOCK_SIZE;
                        bringBlockToBufferIfNotExists(sensorID, blockID);
                        List<SensorRecord> recordsFromBlock = databaseBuffer.get(getDatabaseBufferKey(sensorID, blockID)).getRecords();
                        int entryIndex = (offset - (blockID * BLOCK_SIZE)) / RECORD_SIZE;
                        SensorRecord r = recordsFromBlock.get(entryIndex);

                        logWriter.write("\nLocation MRead: ");
                        printRecordToLog(r);
                    }
                }
            }
        }
    }

    private static void timestampM(int sensorID, long ts1, long ts2) throws IOException {
        TreeMap<Long, ArrayList<Integer>> timeStamp = timeStampIndex.get(sensorID);
        NavigableMap<Long, ArrayList<Integer>> subMap = timeStamp.subMap(ts1, true, ts2, true);

        for (Map.Entry<Long, ArrayList<Integer>> entry : subMap.entrySet()) {
            ArrayList<Integer> timeStampOffsets = entry.getValue();
            for (Integer timeStampOffset : timeStampOffsets) {
                int blockID = timeStampOffset / BLOCK_SIZE;
                bringBlockToBufferIfNotExists(sensorID, blockID);
                List<SensorRecord> recordsFromBlock = databaseBuffer.get(getDatabaseBufferKey(sensorID, blockID)).getRecords();
                int entryIndex = (timeStampOffset - (blockID * BLOCK_SIZE)) / RECORD_SIZE;
                SensorRecord r = recordsFromBlock.get(entryIndex);

                logWriter.write("\nMRead: ");
                printRecordToLog(r);
            }
        }
    }

    private static void G(String operation) throws IOException {
        String[] splitString = operation.split(" ");

        if (splitString[3].equals("T")) {
            long ts1 = Long.parseLong(splitString[4].replaceAll("\\D", ""));
            long ts2 = Long.parseLong(splitString[5].replaceAll("\\D", ""));
            logWriter.write("\nG: " + timestampG(Integer.parseInt(splitString[1]), splitString[2], ts1, ts2));
        } else if (splitString[3].equals("L")) {
            int xVal = Integer.parseInt(splitString[4].replaceAll("\\D", ""));
            int yVal = Integer.parseInt(splitString[5].replaceAll("\\D", ""));
            locationG(Integer.parseInt(splitString[1]), xVal, yVal);
        }
        logWriter.flush();
    }

    private static int timestampG(int sensorID, String op, long ts1, long ts2) {
        ArrayList<Integer> heartRateResult = new ArrayList<>();
        TreeMap<Long, ArrayList<Integer>> timeStamp = timeStampIndex.get(sensorID);
        NavigableMap<Long, ArrayList<Integer>> subMap = timeStamp.subMap(ts1, true, ts2, true);

        for (Map.Entry<Long, ArrayList<Integer>> entry : subMap.entrySet()) {
            ArrayList<Integer> timeStampOffsets = entry.getValue();
            for (Integer timeStampOffset : timeStampOffsets) {
                int blockID = timeStampOffset / BLOCK_SIZE;
                bringBlockToBufferIfNotExists(sensorID, blockID);
                List<SensorRecord> recordsFromBlock = databaseBuffer.get(getDatabaseBufferKey(sensorID, blockID)).getRecords();
                int entryIndex = (timeStampOffset - (blockID * BLOCK_SIZE)) / RECORD_SIZE;
                SensorRecord r = recordsFromBlock.get(entryIndex);

                int heart_val = r.getHeartRate();
                heartRateResult.add(heart_val);
            }
        }
        return aggregationG(op, heartRateResult);
    }

    private static int locationG(int sensorID, int xVal, int yVal) {
        ArrayList<Integer> heartRateResult = new ArrayList<>();
        TreeMap<Integer, ArrayList<Integer>> xLoc = xLocIndex.get(sensorID);
        TreeMap<Integer, ArrayList<Integer>> yLoc = yLocIndex.get(sensorID);
        NavigableMap<Integer, ArrayList<Integer>> xSubMap = xLoc.subMap(xVal, true, yVal, true);
        NavigableMap<Integer, ArrayList<Integer>> ySubMap = xLoc.subMap(xVal, true, yVal, true);
        // TODO figure out how to aggregate when only one point is given
        return 0;
    }

    private static int aggregationG(String op, ArrayList<Integer> heartRates) {
        int result = 0;
        if (op.equals("min")) {
            result = Collections.min(heartRates);
        } else if (op.equals("max")) {
            result = Collections.max(heartRates);
        } else if (op.equals("avg")) {
            result = (int) heartRates.stream().mapToDouble(num -> num).average().orElse(0.0);
        }
        return result;
    }
}
