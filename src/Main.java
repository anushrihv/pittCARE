package src;

import src.dto.Operation;
import src.service.DataManager;
import src.service.TransactionManager;
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

        DataManager.initNumberOfPages(5);

        DataManager.executeOperation(new Operation(1, 1, "I 3 450 550 1678407651 5"));
        DataManager.executeOperation(new Operation(1, 1, "I 3 451 550 1678407651 6"));
        DataManager.executeOperation(new Operation(1, 1, "I 3 452 550 1678407651 7"));
        DataManager.executeOperation(new Operation(1, 1, "C"));
    }
}
