package src;

import src.service.DataManager;
import src.service.TransactionManager;
import src.dto.Record;
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
        System.out.print("Enter the number of pages in the database buffer: ");
        int bufferSize = reader.nextInt();

        reader.close();

        // Testing data manager
        DataManager.initNumberOfPages(bufferSize);
        Record x = new Record();
        x.setSensorID(1);
        x.setHeartRate(88);
        x.setTimestamp(111);
        x.setxLocation(3);
        x.setyLocation(4);

        // Running the Transaction Manager:
        TransactionManager.getTransactions(seed, concurrencyType);
    }
}
