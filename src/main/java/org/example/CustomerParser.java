package org.example;
import org.apache.flink.api.common.functions.MapFunction;

public class CustomerParser implements MapFunction<String, Customer> {
    @Override
    public Customer map(String s) {
        String[] p = s.split("\\|");
        Customer c = new Customer();
        c.c_custkey = Integer.parseInt(p[0]);
        c.c_mktsegment = p[6];
        return c;
    }
}