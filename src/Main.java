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

    }
}
