package org.example;
import org.apache.flink.api.common.functions.MapFunction;
import java.math.BigDecimal;

public class LineitemParser implements MapFunction<String, Lineitem> {
    @Override
    public Lineitem map(String s) {
        String[] p = s.split("\\|");
        Lineitem l = new Lineitem();
        l.l_orderkey = Integer.parseInt(p[0]);
        l.l_extendedprice = new BigDecimal(p[5]);
        l.l_discount = new BigDecimal(p[6]);
        l.l_shipdate = p[10];
        return l;
    }
}