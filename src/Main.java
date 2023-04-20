package src;

import src.dto.Operation;
import src.service.DataManager;
import src.service.TransactionManager;
import src.dto.SensorRecord;

import java.io.IOException;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws IOException {

//        Scanner reader = new Scanner(System.in);
//        System.out.println("Starting pittCARE database...");
//
//        System.out.print("Enter random number generator seed: ");
//        int seed = reader.nextInt();
//        System.out.print("Enter concurrency type ('rr' for round robin and 'r' for random): ");
//        reader.nextLine();
//        String concurrencyType = reader.nextLine();
//
//        reader.close();
//
//        // Running the Transaction Manager:
//        TransactionManager.getTransactions(seed, concurrencyType);
        Scanner reader = new Scanner(System.in);
        System.out.print("Enter random number generator seed: ");
        int seed = reader.nextInt();
        System.out.print("Enter concurrency type ('rr' for round robin and 'r' for random): ");
        reader.nextLine();
        String concurrencyType = reader.nextLine();
        System.out.print("Enter the number of pages in the database buffer: ");
        int bufferSize = reader.nextInt();

        reader.close();

        // Testing data manager
        DataManager.initNumberOfPages(bufferSize);
        SensorRecord x = new SensorRecord();
        x.setSensorID(1);
        x.setHeartRate(88);
        x.setTimestamp(111);
        x.setxLocation(3);
        x.setyLocation(4);

        // Running the Transaction Manager:
        TransactionManager.getTransactions(seed, concurrencyType);
        DataManager.initNumberOfPages(5);

        DataManager.executeOperation(new Operation(1, 1, "I 3 450 550 1678407651 5"));
        DataManager.executeOperation(new Operation(1, 1, "I 3 451 550 1678407651 6"));
        DataManager.executeOperation(new Operation(1, 1, "I 3 452 550 1678407651 7"));
        DataManager.executeOperation(new Operation(1, 1, "C"));
    }
}
