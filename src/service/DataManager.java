package src.service;

import com.sun.source.tree.Tree;
import src.dto.Operation;
import src.dto.Page;
import src.dto.Record;

import java.io.BufferedWriter;
import java.util.*;

import java.io.FileWriter;
import java.io.IOException;

public class DataManager {
    private static LRUCache<String, Page> databaseBuffer;
    private static final Map<Integer, List<Record>> recoveryRecords = new HashMap<>();
    private static final Map<Integer, Integer> recordTracker = new HashMap<>();
    private static int numberOfPages;

    private static Map<Integer, TreeMap<Long, Integer>> timeStampIndex = new HashMap<>();
    private static Map<Integer, TreeMap<Integer, Integer>> xLocIndex = new HashMap<>();
    private static Map<Integer, TreeMap<Integer, Integer>> yLocIndex = new HashMap<>();
    private static Map<Integer, TreeMap<Integer, Integer>> heartRateIndex = new HashMap<>();


    private static BufferedWriter logWriter;

    public static void executeOperation(Operation operation) {
        System.out.println("Operation received for transaction " + operation.getTransactionID() + " for operation " + operation.getOperationStr());
    }

    public static void initNumberOfPages(int pages) {
        numberOfPages = pages;
        databaseBuffer = new LRUCache<>(pages);

        createLogFile();
    }

    public static void readAllRecords(int sensorID, int transactionID) throws IOException {
        int lastBlockInFile = recordTracker.get(sensorID) % 20;

        ArrayList<Record> allRecords = new ArrayList<>();
        for (int i = 0; i < lastBlockInFile; i++) {
            // TODO: bring the block to memory if it doesn't exist

            // Adding all records from this block to allRecords
            List<Record> recordsFromBlock = databaseBuffer.get(sensorID+"-"+i).getRecords();
            for (int j = 0; j < recordsFromBlock.size(); j++) {
                allRecords.add(recordsFromBlock.get(j));
            }
        }
        // Reading from recovery record for same transaction
        List<Record> extraRecords = recoveryRecords.get(transactionID);
        for (int k = 0; k < extraRecords.size(); k++) {
            allRecords.add(extraRecords.get(k));
        }
        // Writing all records to log
        for (int l = 0; l < allRecords.size(); l++) {
            logWriter.write("Read: ");
            printRecordToLog(allRecords.get(l));
        }
    }

    private static void printRecordToLog(Record r) throws IOException {
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
        List<Record> recordsFromBlock = databaseBuffer.get(sensorID+"-"+blockID).getRecords();
        int entryIndex = (offset - (blockID * 480)) / 24;
        Record r = recordsFromBlock.get(entryIndex);
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

                    List<Record> recordsFromBlock = databaseBuffer.get(sensorID+"-"+blockID).getRecords();
                    int entryIndex = (currX - (blockID * 480)) / 24;
                    Record r = recordsFromBlock.get(entryIndex);
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


            List<Record> recordsFromBlock = databaseBuffer.get(sensorID+"-"+blockID).getRecords();
            int entryIndex = (entry.getValue() - (blockID * 480)) / 24;
            Record r = recordsFromBlock.get(entryIndex);
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


            List<Record> recordsFromBlock = databaseBuffer.get(sensorID+"-"+blockID).getRecords();
            int entryIndex = (entry.getValue() - (blockID * 480)) / 24;
            Record r = recordsFromBlock.get(entryIndex);
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
