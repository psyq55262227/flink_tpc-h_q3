package org.example;

import java.io.*;
import java.util.Random;
import java.io.File;

public class DataGenerator {
    static int CUSTOMERS = 10000;
    static int ORDERS = 40000;
    static int LINEITEMS = 100000;

    public static void main(String[] args) throws IOException {
        File dir = new File("data");
        if (!dir.exists()) dir.mkdirs();

        System.out.println("Generating data...");
        genCust("data/customer.tbl");
        genOrder("data/orders.tbl");
        genLine("data/lineitem.tbl");
        System.out.println("Done. Files created in data/");
    }

    private static void genCust(String path) throws IOException {
        PrintWriter pw = new PrintWriter(new FileWriter(path));
        Random r = new Random();
        for (int i = 1; i <= CUSTOMERS; i++) {
            String seg = r.nextBoolean() ? "BUILDING" : "OTHER";
            pw.println(i + "|User" + i + "|Addr|1|123|0.0|" + seg + "|comment");
        }
        pw.close();
    }

    private static void genOrder(String path) throws IOException {
        PrintWriter pw = new PrintWriter(new FileWriter(path));
        Random r = new Random();
        for (int i = 1; i <= ORDERS; i++) {
            int custKey = r.nextInt(CUSTOMERS) + 1;
            String date = r.nextBoolean() ? "1995-03-01" : "1995-04-01";
            pw.println(i + "|" + custKey + "|X|0.0|" + date + "|P|C|0|comment");
        }
        pw.close();
    }

    private static void genLine(String path) throws IOException {
        PrintWriter pw = new PrintWriter(new FileWriter(path));
        Random r = new Random();
        for (int i = 1; i <= LINEITEMS; i++) {
            int orderKey = r.nextInt(ORDERS) + 1;
            double price = r.nextDouble() * 100;
            double disc = r.nextDouble() * 0.1;
            String shipDate = r.nextBoolean() ? "1995-03-01" : "1995-04-01";
            pw.println(orderKey + "|1|1|1|1|" + String.format("%.2f", price) + "|" + String.format("%.2f", disc) + "|0|0|N|" + shipDate + "|SHIP|AIR|comment");
        }
        pw.close();
    }
}