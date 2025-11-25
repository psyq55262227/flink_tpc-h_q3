package org.example;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import java.math.BigDecimal;
import java.util.Random;

public class PerformanceTestSource extends RichParallelSourceFunction<UpdateEvent<FinalResult>> {

    private volatile boolean run = true;
    private final int count = 2000000;

    @Override
    public void run(SourceContext<UpdateEvent<FinalResult>> ctx) throws Exception {
        int idx = getRuntimeContext().getIndexOfThisSubtask();
        int step = getRuntimeContext().getNumberOfParallelSubtasks();
        Random r = new Random(idx);

        for (int i = idx; i < count && run; i += step) {
            FinalResult res = new FinalResult();
            res.orderKey = r.nextInt(5000);
            res.orderDate = "1995-01-01";
            res.shipPriority = 0;
            res.revenue = BigDecimal.valueOf(r.nextDouble() * 1000);

            ctx.collect(new UpdateEvent<>(OpType.INSERT, res));
        }
    }

    @Override
    public void cancel() {
        run = false;
    }
}