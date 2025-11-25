package org.example;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Collector;

import java.io.PrintWriter;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class IncrementalJoinJob {

    public static final String MODE = "PERFORMANCE";

    private static final int PER_CORE_CUST = 20000;
    private static final int PER_CORE_ORD  = 80000;
    private static final int PER_CORE_LINE = 200000;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("heartbeat.timeout", "60000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        int parallelism = 4;
        if (args.length > 0) {
            parallelism = Integer.parseInt(args[0]);
        }

        if (MODE.equals("PERFORMANCE")) {
            env.setParallelism(parallelism);
        } else {
            env.setParallelism(1);
        }

        DataStream<UpdateEvent<Customer>> cStream;
        DataStream<UpdateEvent<Orders>> oStream;
        DataStream<UpdateEvent<Lineitem>> lStream;

        if (MODE.equals("PERFORMANCE")) {
            cStream = env.addSource(new MemCustomerSource()).map(new CPULoadMap<>());
            oStream = env.addSource(new MemOrderSource()).map(new CPULoadMap<>());
            lStream = env.addSource(new MemLineitemSource()).map(new CPULoadMap<>());
        } else {
            cStream = env.readTextFile("data/customer.tbl").map(new CustomerParser()).map(new MapFunction<Customer, UpdateEvent<Customer>>() {@Override public UpdateEvent<Customer> map(Customer c) { return new UpdateEvent<>(OpType.INSERT, c); }});
            oStream = env.readTextFile("data/orders.tbl").map(new OrdersParser()).map(new MapFunction<Orders, UpdateEvent<Orders>>() {@Override public UpdateEvent<Orders> map(Orders o) { return new UpdateEvent<>(OpType.INSERT, o); }});
            lStream = env.readTextFile("data/lineitem.tbl").map(new LineitemParser()).map(new MapFunction<Lineitem, UpdateEvent<Lineitem>>() {@Override public UpdateEvent<Lineitem> map(Lineitem l) { return new UpdateEvent<>(OpType.INSERT, l); }});
        }

        DataStream<UpdateEvent<CustomerOrder>> tmpJoin = cStream.keyBy(e -> e.payload.c_custkey)
                .connect(oStream.keyBy(e -> e.payload.o_custkey))
                .process(new Join1());

        DataStream<UpdateEvent<FinalResult>> finalJoin = tmpJoin.keyBy(e -> e.payload.o_orderkey)
                .connect(lStream.keyBy(e -> e.payload.l_orderkey))
                .process(new Join2());

        DataStream<String> outputStream = finalJoin.keyBy(new KeySelector<UpdateEvent<FinalResult>, Tuple3<Integer, String, Integer>>() {
            @Override public Tuple3<Integer, String, Integer> getKey(UpdateEvent<FinalResult> e) throws Exception {
                return Tuple3.of(e.payload.orderKey, e.payload.orderDate, e.payload.shipPriority);
            }
        }).process(new AggProcess());

        if (MODE.equals("PERFORMANCE")) {
            outputStream.addSink(new DiscardingSink<>());

            System.out.println("Starting Performance Test (P=" + env.getParallelism() + ")...");
            JobExecutionResult res = env.execute("Performance Test");

            long cost = res.getNetRuntime(TimeUnit.MILLISECONDS);

            long totalEvents = (long)(PER_CORE_CUST + PER_CORE_ORD + PER_CORE_LINE) * env.getParallelism();
            double seconds = cost / 1000.0;
            long tps = (long) (totalEvents / seconds);

            System.out.println("Total Events Processed: " + totalEvents);
            System.out.println("Time: " + cost + " ms");
            System.out.println("Throughput: " + tps + " updates/s");
        } else {
            System.out.println("Starting Verification...");
            Iterator<String> iter = DataStreamUtils.collect(outputStream);
            TreeMap<Integer, String> sortedResults = new TreeMap<>();
            while (iter.hasNext()) {
                String line = iter.next();
                int firstPipe = line.indexOf('|');
                if (firstPipe != -1) {
                    int orderKey = Integer.parseInt(line.substring(0, firstPipe));
                    sortedResults.put(orderKey, line);
                }
            }
            PrintWriter pw = new PrintWriter("flink_result.txt");
            for (String line : sortedResults.values()) pw.println(line);
            pw.close();
            System.out.println("Verification done.");
        }
    }

    public static class MemCustomerSource extends RichParallelSourceFunction<UpdateEvent<Customer>> {
        private volatile boolean run = true;
        @Override public void run(SourceContext<UpdateEvent<Customer>> ctx) {
            for (int i = 0; i < PER_CORE_CUST && run; i++) {
                Customer c = new Customer(); c.c_custkey = i; c.c_mktsegment = "BUILDING";
                ctx.collect(new UpdateEvent<>(OpType.INSERT, c));
            }
        }
        @Override public void cancel() { run = false; }
    }
    public static class MemOrderSource extends RichParallelSourceFunction<UpdateEvent<Orders>> {
        private volatile boolean run = true;
        @Override public void run(SourceContext<UpdateEvent<Orders>> ctx) {
            Random r = new Random();
            for (int i = 0; i < PER_CORE_ORD && run; i++) {
                Orders o = new Orders(); o.o_orderkey = i; o.o_custkey = r.nextInt(PER_CORE_CUST); o.o_orderdate = "1995-01-01"; o.o_shippriority = 0;
                ctx.collect(new UpdateEvent<>(OpType.INSERT, o));
            }
        }
        @Override public void cancel() { run = false; }
    }
    public static class MemLineitemSource extends RichParallelSourceFunction<UpdateEvent<Lineitem>> {
        private volatile boolean run = true;
        @Override public void run(SourceContext<UpdateEvent<Lineitem>> ctx) {
            Random r = new Random();
            for (int i = 0; i < PER_CORE_LINE && run; i++) {
                Lineitem l = new Lineitem(); l.l_orderkey = r.nextInt(PER_CORE_ORD); l.l_extendedprice = new BigDecimal("100.00"); l.l_discount = new BigDecimal("0.10"); l.l_shipdate = "1995-01-01";
                ctx.collect(new UpdateEvent<>(OpType.INSERT, l));
            }
        }
        @Override public void cancel() { run = false; }
    }

    public static class CPULoadMap<T> implements MapFunction<UpdateEvent<T>, UpdateEvent<T>> {
        @Override
        public UpdateEvent<T> map(UpdateEvent<T> event) {
            double dummy = 1.0;
            for (int i = 0; i < 20000; i++) {
                dummy = (dummy * 1.0001) / 0.9999;
            }
            if (dummy < 0) System.out.print("");
            return event;
        }
    }

    public static class Join1 extends CoProcessFunction<UpdateEvent<Customer>, UpdateEvent<Orders>, UpdateEvent<CustomerOrder>> {
        private MapState<Integer, Customer> cState;
        private MapState<Integer, List<Orders>> oState;
        @Override
        public void open(Configuration parameters) {
            cState = getRuntimeContext().getMapState(new MapStateDescriptor<>("c", Integer.class, Customer.class));
            oState = getRuntimeContext().getMapState(new MapStateDescriptor<>("o", TypeInformation.of(Integer.class), TypeInformation.of(new TypeHint<List<Orders>>() {})));
        }
        @Override
        public void processElement1(UpdateEvent<Customer> event, Context ctx, Collector<UpdateEvent<CustomerOrder>> out) throws Exception {
            Customer c = event.payload;
            if (event.opType == OpType.INSERT) {
                cState.put(c.c_custkey, c);
                if (oState.contains(c.c_custkey)) for (Orders o : oState.get(c.c_custkey)) join(c, o, OpType.INSERT, out);
            } else {
                cState.remove(c.c_custkey);
                if (oState.contains(c.c_custkey)) for (Orders o : oState.get(c.c_custkey)) join(c, o, OpType.DELETE, out);
            }
        }
        @Override
        public void processElement2(UpdateEvent<Orders> event, Context ctx, Collector<UpdateEvent<CustomerOrder>> out) throws Exception {
            Orders o = event.payload;
            List<Orders> list = oState.get(o.o_custkey);
            if (list == null) list = new ArrayList<>();
            if (event.opType == OpType.INSERT) {
                list.add(o);
                if (cState.contains(o.o_custkey)) join(cState.get(o.o_custkey), o, OpType.INSERT, out);
            } else {
                list.removeIf(x -> x.o_orderkey == o.o_orderkey);
                if (cState.contains(o.o_custkey)) join(cState.get(o.o_custkey), o, OpType.DELETE, out);
            }
            oState.put(o.o_custkey, list);
        }
        private void join(Customer c, Orders o, OpType op, Collector<UpdateEvent<CustomerOrder>> out) {
            if (!"BUILDING".equals(c.c_mktsegment)) return;
            if (o.o_orderdate.compareTo("1995-03-15") >= 0) return;
            CustomerOrder co = new CustomerOrder();
            co.o_orderkey = o.o_orderkey;
            co.o_orderdate = o.o_orderdate;
            co.o_shippriority = o.o_shippriority;
            out.collect(new UpdateEvent<>(op, co));
        }
    }

    public static class Join2 extends CoProcessFunction<UpdateEvent<CustomerOrder>, UpdateEvent<Lineitem>, UpdateEvent<FinalResult>> {
        private MapState<Integer, CustomerOrder> coState;
        private MapState<Integer, List<Lineitem>> lState;
        @Override
        public void open(Configuration parameters) {
            coState = getRuntimeContext().getMapState(new MapStateDescriptor<>("co", Integer.class, CustomerOrder.class));
            lState = getRuntimeContext().getMapState(new MapStateDescriptor<>("l", TypeInformation.of(Integer.class), TypeInformation.of(new TypeHint<List<Lineitem>>() {})));
        }
        @Override
        public void processElement1(UpdateEvent<CustomerOrder> event, Context ctx, Collector<UpdateEvent<FinalResult>> out) throws Exception {
            CustomerOrder co = event.payload;
            if (event.opType == OpType.INSERT) {
                coState.put(co.o_orderkey, co);
                if (lState.contains(co.o_orderkey)) for (Lineitem l : lState.get(co.o_orderkey)) join(co, l, OpType.INSERT, out);
            } else {
                coState.remove(co.o_orderkey);
                if (lState.contains(co.o_orderkey)) for (Lineitem l : lState.get(co.o_orderkey)) join(co, l, OpType.DELETE, out);
            }
        }
        @Override
        public void processElement2(UpdateEvent<Lineitem> event, Context ctx, Collector<UpdateEvent<FinalResult>> out) throws Exception {
            Lineitem l = event.payload;
            List<Lineitem> list = lState.get(l.l_orderkey);
            if (list == null) list = new ArrayList<>();
            if (event.opType == OpType.INSERT) {
                list.add(l);
                if (coState.contains(l.l_orderkey)) join(coState.get(l.l_orderkey), l, OpType.INSERT, out);
            } else {
                list.removeIf(x -> x.l_extendedprice.equals(l.l_extendedprice) && x.l_discount.equals(l.l_discount));
                if (coState.contains(l.l_orderkey)) join(coState.get(l.l_orderkey), l, OpType.DELETE, out);
            }
            lState.put(l.l_orderkey, list);
        }
        private void join(CustomerOrder co, Lineitem l, OpType op, Collector<UpdateEvent<FinalResult>> out) {
            if (l.l_shipdate.compareTo("1995-03-15") <= 0) return;
            FinalResult res = new FinalResult();
            res.orderKey = co.o_orderkey;
            res.orderDate = co.o_orderdate;
            res.shipPriority = co.o_shippriority;
            res.revenue = l.l_extendedprice.multiply(BigDecimal.ONE.subtract(l.l_discount));
            out.collect(new UpdateEvent<>(op, res));
        }
    }

    public static class AggProcess extends KeyedProcessFunction<Tuple3<Integer, String, Integer>, UpdateEvent<FinalResult>, String> {
        private ValueState<BigDecimal> sumState;
        @Override
        public void open(Configuration parameters) {
            sumState = getRuntimeContext().getState(new ValueStateDescriptor<>("sum", BigDecimal.class));
        }
        @Override
        public void processElement(UpdateEvent<FinalResult> event, Context ctx, Collector<String> out) throws Exception {
            BigDecimal val = sumState.value();
            if (val == null) val = BigDecimal.ZERO;
            if (event.opType == OpType.INSERT) val = val.add(event.payload.revenue);
            else val = val.subtract(event.payload.revenue);
            sumState.update(val);
            String output = ctx.getCurrentKey().f0 + "|" + ctx.getCurrentKey().f1 + "|" + ctx.getCurrentKey().f2 + "|" + val.setScale(2, BigDecimal.ROUND_HALF_UP);
            out.collect(output);
        }
    }
}