package src;

import src.service.TransactionManager;

import java.io.IOException;
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

        reader.close();

        // Running the Transaction Manager:
        TransactionManager.getTransactions(seed, concurrencyType);
    }
}
