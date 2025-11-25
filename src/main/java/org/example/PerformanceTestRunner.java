package org.example;

import java.io.*;
import java.util.concurrent.TimeUnit;

public class PerformanceTestRunner {

    private static final int[] PARAS = {1, 4, 8};

    public static void main(String[] args) {
        System.out.println("- Starting performance test -");

        try {
            for (int p : PARAS) {
                System.out.println("Testing with parallelism: " + p);
                runTest(p);
                Thread.sleep(2000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("All tests done.");
    }

    private static void runTest(int para) throws Exception {
        String cp = System.getProperty("java.class.path");
        String java = System.getProperty("java.home") + "/bin/java";

        ProcessBuilder pb = new ProcessBuilder(
                java,
                "--add-opens=java.base/java.util=ALL-UNNAMED",
                "--add-opens=java.base/java.lang=ALL-UNNAMED",
                "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
                "-cp", cp,
                "-Xmx2g",
                "org.example.IncrementalJoinJob",
                String.valueOf(para)
        );

        pb.redirectErrorStream(true);
        Process proc = pb.start();

        BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.contains("Time") || line.contains("Throughput") || line.contains("Total Events") || line.contains("Processing")) {
                System.out.println(line);
            }
            if (line.contains("Exception") || line.contains("Error") || line.contains("Caused by")) {
                System.out.println(line);
            }
        }

        proc.waitFor(5, TimeUnit.MINUTES);
    }
}