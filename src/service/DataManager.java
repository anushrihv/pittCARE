package src.service;

import src.dto.Operation;
import src.dto.Page;
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
    }


}
