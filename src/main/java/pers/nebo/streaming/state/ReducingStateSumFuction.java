package pers.nebo.streaming.state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @ auther fnb
 * @ email nebofeng@gmail.com
 * @ date  2020/6/11
 * @ des :
 *
 *

 *  ReducingState<T> ：这个状态为每一个 key 保存一个聚合之后的值
 *      get() 获取状态值
 *      add()  更新状态值，将数据放到状态中
 *      clear() 清除状态

 */
public class ReducingStateSumFuction  extends RichFlatMapFunction<Tuple2<Long,Long>,Tuple2<Long,Long>> {


    private ReducingState<Long> sumState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ReducingStateDescriptor<Long> descriptor = new ReducingStateDescriptor<Long>(
                "sum",
                new ReduceFunction<Long>() {
                    @Override
                    public Long reduce(Long value1, Long value2) throws Exception {
                        return  value1+value2;
                    }
                }
        ,Long.class);


        sumState=getRuntimeContext().getReducingState(descriptor);


    }

    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
        sumState.add(value.f1);
        out.collect(Tuple2.of(value.f0,sumState.get()));

    }
}
