# pittCARE

## Project description

### Transaction Manager

* A Transaction Manager uses a Transaction table to keep track of all the active transactions and a list of fileNames to keep track of all the open files
* When a transaction begins, the transaction manager generates a new transaction Id for it
* A user specifies the type of concurrent reading mechanism for the files: Round Robin or Random
* Data Structure
  * Transaction table: A map between a filename and TransactionDetails. Once a transaction is committed or aborted, the entry is removed from the transaction table
  * Open files: List of all the open files. Once a file is terminated, or a transaction is committed or aborted, the entry is removed from this list
  ```
  class TransactionDetails{
      int transactionID,
      int lineNumberRead,
      int isolationLevel;
  }

  HashMap<String, <TransactionDetails>> transactionTable;
  LinkedList<String> fileNames;
 * isolationLevel 0 represents a process and 1 represents a transaction
 * The transaction manager sends the operation to the scheduler using the following structure:
   ```
   class Operation {
   int transactionID,
   int isolationLevel,
   String operation
   }
   ```
### Scheduler
* A scheduler is responsible for scheduling the operations from transactions
* A scheduler implements the Lock table and Deadlock detector. There are 2 kinds of locks used: READLOCK, WRITELOCK. Both ReadLock and WriteLock are used at the File level
  ```
  enum Lock {
  READLOCK,
  WRITELOCK
  }
* Lock table is used to keep track of different locks granted for a data item. It is a map between the data item and the LockDetails
  ```
  class LockDetails {
   List<TransactionLock> locksGranted;
   List<TransactionLock> locksRequested;
  }

  class TransactionLock {
    int transactionID;
    Lock locktype;
    String operation;
  }

  HashMap<Integer, LockDetails> LockTable = new HashMap<>();
* A compatibility table is used to determine if two transactions are compatible or conflicting based on the locks they hold or request. We store it in the form of a 2 dimensional array
  ```
  boolean[][] transactionCompatibilityTable = new boolean[2][2];
* Scheduler ensures Strict 2 Phase Locking by keeping the locks until commit time for a transaction
* The Scheduler sends the Operation to the Data Manager

### Data Manager
* The Data Manager is responsible for interpreting and executing the operations on the data, managing the data blocks, and managing the memory buffer pages
* A data block is 400 bytes
* The user enters the number of pages in the memory buffer, where each page holds data equivalent to a data block in the file. A record will only be inserted in a block if there is enough space.
* Upon receiving a read or write request, the required data blocks are loaded into memory before any operations are performed. In the case of a new record, the tuple is constructed entirely in memory using a recovery record.
* File organization in the form of data blocks
  * Records in the file are serialized and inserted. The offset for a record can be easily retrieved by multiplying the recordID and the max size of each record (24 bytes).
* Memory buffer is organized in the form of pages. The Data Manager maintains a PriorityQueue of Page objects, with each page assigned a priority based on its level of recent usage(LRU). Pages with a higher priority will be evicted from the queue later, allowing frequently accessed pages to remain in memory longer. Priorities are updated as pages are accessed.
  ```
  class Record {
   int sensorID,
   int xLocation,
   int yLocation,
   long timestamp,
   int heartRate
  }

  class Page {
   int priority,
   int blockNumber,
   List<Record> record;
   }

  PriorityQueue<Page> databaseBuffer = new PriorityQueue<>();
* Auxiliary structures
  * RecoveryRecord: The recovery record tracks the actions of any uncommitted transaction. If a transaction is committed, the RecoveryRecord is brought into the memory buffer and flushed to the file. If a transaction aborts, its entry is removed from the RecoveryRecord. It is stored as a map between the transactionID and the Record
    ```
    Map<Integer, List<Record>> recoveryRecord = new HashMap<>();
  * Indices for each sensor Id
    ```
    Map<Integer, TreeMap<int, int>> timeStampIndex = new Map<>();
    Map<Integer, TreeMap<int, int>> xLocIndex = new Map<>();
    Map<Integer, TreeMap<int, int>> yLocStampIndex = new Map<>();
  * Index for time window retrieval: To efficiently retrieve records within a specified time window, we will implement an index using a TreeMap in Java. The TreeMap will keep track of timestamps as keys, sorted in ascending order, and the corresponding record IDs as values
    ```
    TreeMap<int, int> timeStamp = new TreeMap<>();
  * Index for location based retrieval: To enable efficient location-based retrieval, we use two sorted indices: one for x-axis values and one for y-axis values. Each index is implemented using a TreeMap, where the keys are sorted values and the values are the associated record IDs
    ```
    TreeMap<int, int> xLocation = new TreeMap<>();
    TreeMap<int, int> yLocation = new TreeMap<>();
  * RecordTracker: A map between the sensor ID and the number of records inserted already

## Building and running the application

Build the application from the root of the project using 

` javac src/Main.java`

Run the application using

`java src/Main`
