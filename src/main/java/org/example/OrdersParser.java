package org.example;
import org.apache.flink.api.common.functions.MapFunction;

public class OrdersParser implements MapFunction<String, Orders> {
    @Override
    public Orders map(String s) {
        String[] p = s.split("\\|");
        Orders o = new Orders();
        o.o_orderkey = Integer.parseInt(p[0]);
        o.o_custkey = Integer.parseInt(p[1]);
        o.o_orderdate = p[4];
        o.o_shippriority = Integer.parseInt(p[7]);
        return o;
    }
}