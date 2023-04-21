package src;

import src.dto.Operation;
import src.dto.Page;
import src.service.DataManager;
import src.service.TransactionManager;
import src.dto.SensorRecord;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws IOException {

        Scanner reader = new Scanner(System.in);
        System.out.println("Starting pittCARE database...");

        System.out.print("Enter random number generator seed: ");
        int seed = reader.nextInt();
        System.out.print("Enter concurrency type ('rr' for round robin and 'r' for random): ");
        reader.nextLine();
        String concurrencyType = reader.nextLine();
        System.out.print("Enter the number of pages in the database buffer: ");
        int bufferSize = reader.nextInt();

        reader.close();

        DataManager.initNumberOfPages(bufferSize);
        // Running the Transaction Manager:
        TransactionManager.getTransactions(seed, concurrencyType);
//
//        DataManager.executeOperation(new Operation(1, 1, "I 3 30 30 1678407650 5"));
//        DataManager.executeOperation(new Operation(1, 1, "I 3 30 31 1678407650 6"));
//        DataManager.executeOperation(new Operation(1, 1, "I 3 32 32 1678407650 6"));
//
//        DataManager.executeOperation(new Operation(1, 1, "I 3 31 31 1678407651 6"));
//        DataManager.executeOperation(new Operation(1, 1, "I 3 29 30 1678407651 8"));
//        DataManager.executeOperation(new Operation(1, 1, "I 3 28 30 1678407652 9"));
//        DataManager.executeOperation(new Operation(1, 1, "C"));
//        DataManager.executeOperation(new Operation(1, 1, "M 3 H 6"));
//        DataManager.executeOperation(new Operation(1, 1, "G 3 max T [1678407651, 1678407652]"));
//        DataManager.executeOperation(new Operation(1, 1, "M 3 L [30, 30]"));
//        DataManager.executeOperation(new Operation(1, 1, "R 3"));
//        DataManager.executeOperation(new Operation(1, 1, "M 3 T [1678407650, 1678407652]"));


    }
}
