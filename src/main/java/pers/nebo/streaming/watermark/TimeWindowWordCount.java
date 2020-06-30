package pers.nebo.streaming.watermark;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/**
 * @ auther fnb
 * @ email nebofeng@gmail.com
 * @ date  2020/6/30
 * @ des : time window
 */
public class TimeWindowWordCount {
    public static void main(String[] args) throws  Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource=env.socketTextStream("192.168.192.1",8888);
        SingleOutputStreamOperator<Tuple2<String,Integer>> result=dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String [] fields = value.split(",");
                for(String word:fields){
                    out.collect(new Tuple2<>(word,1));
                }
            }
        }).keyBy(0)
                .timeWindow(Time.seconds(10),Time.seconds(5))
                .sum(1);

        result.print().setParallelism(1);

        env.execute("TimeWindowWordCount");

    }
}
