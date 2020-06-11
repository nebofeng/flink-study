package pers.nebo.streaming.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;


/**
 * @ auther fnb
 * @ email nebofeng@gmail.com
 * @ date  2020/6/8
 * @ des :
 *
 *  *  ValueState<T> ：这个状态为每一个 key 保存一个值
 *  *      value() 获取状态值
 *  *      update() 更新状态值
 *  *      clear() 清除状态
 */
public class CountWindowAverageWithValueState extends RichFlatMapFunction<Tuple2<Long,Long> ,Tuple2<Long,Double>> {

    // 用以保存每个 key 出现的次数，以及这个 key 对应的 value 的总值
    // managed keyed state
    //1. ValueState 保存的是对应的一个 key 的一个状态值
    private ValueState<Tuple2<Long,Long>>  countAndSum;


    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Tuple2<Long,Long>> descriptor=new ValueStateDescriptor<Tuple2<Long, Long>>(
                "average", //statename
                Types.TUPLE(Types.LONG,Types.LONG));// 状态存储的数据类型

        countAndSum=getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Double>> out) throws Exception {
        Tuple2<Long,Long> currentState =countAndSum.value();
        if(currentState==null){
            currentState=Tuple2.of(0L,0L);

        }
        currentState.f0+=1;
        currentState.f1+=value.f1;
        countAndSum.update(currentState);

        if(currentState.f0>=3){
            double avg=(double)currentState.f1/currentState.f0;
            out.collect(Tuple2.of(value.f0,avg));

            countAndSum.clear();
        }

    }
    /**


     dataStreamSource
     .keyBy(0)
     .flatMap(new CountWindowAverageWithListState())
     .print();

     */

    //调用


}
