# Implementing Continuous Query Processing for TPC-H Q3 on Apache Flink
## 1. Project Overview
The system supports:
*   **Verification Mode:** Verifies correctness against a PostgreSQL database.
*   **Performance Mode:** Tests throughput scalability using in-memory data generation and CPU load simulation.

## 2. Project Structure

*   `src/main/java/org/example/IncrementalJoinJob.java`: The main Flink application containing the Join topology, State management, and Aggregation logic.
*   `src/main/java/org/example/PerformanceTestRunner.java`: A utility script to automate performance testing with different parallelism levels.
*   `src/main/java/org/example/DataGenerator.java`: Generates `.tbl` files for correctness verification.
*   `data/`: Directory for storing generated test data (excluded from version control).

## 3. Prerequisites

*   **Java 17** (Required for `--add-opens` compatibility)
*   **Maven** (or IntelliJ IDEA build system)
*   **IntelliJ IDEA** (Recommended for running)

## 4. How to Run

### 4.1. Generate Test Data
Before running the verification mode, you must generate the local `.tbl` files.

1.  Run the `src/main/java/org/example/DataGenerator.java` class.
2.  It will create `customer.tbl`, `orders.tbl`, and `lineitem.tbl` in the `data/` directory.

### 4.2. Run Correctness Verification
To verify that the algorithm produces correct results:

1.  Open `src/main/java/org/example/IncrementalJoinJob.java`.
2.  Change the `MODE` constant to `"VERIFICATION"`:
    ```java
    public static final String MODE = "VERIFICATION";
    ```
3.  Run the `main` method.
4.  The result will be saved to `flink_result.txt` in the project root.
5.  (Optional) You can compare this file with SQL output using the `fc` command on Windows.

### 4.3. Run Performance Test
To reproduce the scalability results (Weak Scaling) presented in the report:

1.  Open `src/main/java/org/example/PerformanceTestRunner.java`.
2.  Run the `main` method directly.
3.  The runner will automatically:
    *   Set `MODE` to `"PERFORMANCE"`.
    *   Run the job with Parallelism = 1, 4, and 8 sequentially.
    *   Pass necessary JVM arguments (`--add-opens`) for Java 17.
4.  The console will output the **Time** and **Throughput (updates/s)** for each scenario.
