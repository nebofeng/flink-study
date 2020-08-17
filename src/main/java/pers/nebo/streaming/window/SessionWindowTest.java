package pers.nebo.streaming.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @ auther fnb
 * @ email nebofeng@gmail.com
 * @ date  2020/8/12
 * @ des :
 */
public class SessionWindowTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource= env.socketTextStream("localhost",8888);
        SingleOutputStreamOperator<Tuple2<String,Integer>> stream=dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fields=value.split(",");
                for(String word:fields){
                    out.collect(Tuple2.of(word,1));
                }

            }
        });


        stream.keyBy(0)
                //每隔4s 统计最近6s中的数据
                .timeWindow(Time.seconds(6),Time.seconds(4))
                .sum(1)
                .print();

        stream.keyBy(0)
                //每隔4s 统计最近6s中的数据
                .timeWindow(Time.seconds(6))
                .sum(1)
                .print();


        stream.keyBy(0)
                //每隔4条 数据，统计最近 6条数据
                .countWindow(6,4).sum(1).print();

        stream.keyBy(0)
                //每隔4条 数据，统计最近 6条数据
                .countWindow(6).sum(1).print();

        //session Window
        //超过5s 这个单词不出现，就打印出来。
        stream.keyBy(0)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .sum(1)
                .print();

        //timewindow 还有session window  count window 只有 滑动 、滚动 。
        //因为 time window 有时间 ，而countwindow 只有次数


        //滑动窗口

        stream.keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .sum(1)
                .print();
    }
}
