package src.service;

import java.io.*;
import java.util.*;

import src.dto.TransactionDetails;
import src.dto.Operation;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class TransactionManager {
    private static LinkedList<String> fileNames = new LinkedList<>();
    private static HashMap<String, TransactionDetails> transactionTable = new HashMap<>();
    private static int tID;
    private static ArrayList<String> files = new ArrayList<>();

    public static void getTransactions(int seed, String concurrencyType) throws IOException {
        tID = 0;
        readInputFiles();
        //System.out.println("All input files: "+ files);
        if (concurrencyType.equals("rr")) {
            roundRobin();
        } else if (concurrencyType.equals("r")) {
            randomReading(seed);
        }
    }

    private static void roundRobin() throws IOException {
        // Opening a BufferedReader for each file
        BufferedReader[] readers = new BufferedReader[files.size()];
        for (int i = 0; i < files.size(); i++) {
            readers[i] = new BufferedReader(new FileReader(files.get(i)));
        }
        String[] operations = new String[readers.length];
        // Read the first line from each file: B
        for (int j = 0; j < readers.length; j++) {
            operations[j] = readers[j].readLine();
        }

        while (true) {
            boolean allFilesRead = true;

            for (int k = 0; k < readers.length; k++) {
                if (operations[k] != null) {
                    String[] splitString = operations[k].split(" ");
                    if (splitString[0].equals("B")) {
                        addToTransactionTable(files.get(k), splitString[1]);
                        fileNames.add(files.get(k));
                    } else {
                        Operation op = new Operation();
                        op.setTransactionID(transactionTable.get(files.get(k)).getTransactionID());
                        op.setIsolationLevel(transactionTable.get(files.get(k)).getIsolationLevel());
                        op.setOperationStr(operations[k]);
                        System.out.println(op.toString());
                        //TODO Send operation to Scheduler
                        Scheduler.scheduleOperation(op);
                        if (splitString[0].equals("C") || splitString[0].equals("A")) {
                            transactionTable.remove(files.get(k));
                            fileNames.remove(files.get(k));
                        }
                    }
                    // read next line in file through buffered reader:
                    operations[k] = readers[k].readLine();
                    allFilesRead = false;
                }
            }
            if (allFilesRead) {
                break;
            }
        }
        // Closing the BufferedReaders
        for (BufferedReader reader : readers) {
            reader.close();
        }
    }

    private static void addToTransactionTable(String fileName, String isolationLevel) {
        TransactionDetails transaction = new TransactionDetails();
        transaction.setTransactionID(tID);
        if (isolationLevel.equals("1")) {
            transaction.setIsolationLevel(1);
        } else if (isolationLevel.equals("0")) {
            transaction.setIsolationLevel(0);
        }
        transaction.setLineNumberRead(1);
        transactionTable.put(fileName, transaction); // adding transaction to transaction table
        tID++;
    }

    private static void randomReading(int seed) throws IOException {
        Random rand = new Random(seed);

        // Opening a BufferedReader for each file
        BufferedReader[] readers = new BufferedReader[files.size()];

        int filesIP = files.size();

        while (filesIP != 0) {
            // Opening a random file to read
            int index = rand.nextInt(files.size());
            String randomFile = files.get(index);
            // Initializing BufferedReader object for the file if it doesn't exist
            if (readers[index].ready()) {
                readers[index] = new BufferedReader(new FileReader(randomFile));
            }
            // Getting a random number of lines to ready from the file with upper bound of total lines in files
            // TODO should we change upper bound to total lines subtracted by lines read???????????
            int randomNumOfLines = rand.nextInt((int) totalLinesInFile(randomFile));

            for (int i = 0; i < randomNumOfLines; i++) {
                if (readers[index].ready()) {
                    String operation = readers[index].readLine();
                    String[] splitString = operation.split(" ");
                    if (splitString[0].equals("B")) {
                        addToTransactionTable(files.get(index), splitString[1]);
                        fileNames.add(files.get(index));
                    } else {
                        Operation op = new Operation();
                        op.setTransactionID(transactionTable.get(files.get(index)).getTransactionID());
                        op.setIsolationLevel(transactionTable.get(files.get(index)).getIsolationLevel());
                        op.setOperationStr(operation);
                        System.out.println(op);
                        //TODO Send operation to Scheduler
                        Scheduler.scheduleOperation(op);
                        if (splitString[0].equals("C") || splitString[0].equals("A")) {
                            transactionTable.remove(files.get(index));
                            fileNames.remove(files.get(index));
                        }
                    }
                } else {
                    filesIP--; // remove a file count from files in progress (filesIP)
                    break; // break if no lines left to read
                }
            }
        }

        // TODO change duplicate code, modularize

        // Closing the BufferedReaders
        for (BufferedReader reader : readers) {
            reader.close();
        }

    }

    public static long totalLinesInFile(String filename) throws IOException {
        return Files.lines(Paths.get(filename)).count();
    }

    private static void readInputFiles() {
        Path inputDir = Paths.get("src", "input");
        try (Stream<Path> stream = Files.list(inputDir)) {
            stream.forEach(path -> {
                //                    String contents = Files.readString(path);
                //files.add(String.valueOf(path.getFileName()));
                files.add(String.valueOf(path));
                //                    System.out.println("Contents of " + path.getFileName() + ":");
                //                    System.out.println(contents);
            });
        } catch (IOException e) {
            System.out.println("An error occurred while listing the files in the directory: " + e.getMessage());
        }
    }

}
