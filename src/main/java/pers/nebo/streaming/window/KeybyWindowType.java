package pers.nebo.streaming.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**
 * @ auther fnb
 * @ email nebofeng@gmail.com
 * @ date  2020/8/11
 * @ des :
 *
 *
 */
public class KeybyWindowType {
/**
 * Window:
 *   Non Keyed Window
 *   Keyed Window
 *
 *  State：
 *    Keyed State
 *    Operator state （Non Keyed State）
 *
 *  Stream：
 *   Keyed Stream
 *   Non Keyed Stream
 *
 *
 */

public static void main(String[] args)  throws  Exception {
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
    //Non keyed window
//    AllWindowedStream<Tuple2<String,Integer>, TimeWindow> nonkeyedStream =stream.timeWindowAll(Time.seconds(3));
//    nonkeyedStream.sum(1)
//            .print();

    //keyed Stream
    stream.keyBy(0)
            .timeWindow(Time.seconds(3))
            .sum(1)
            .print();
    env.execute("Word Count");

}


}
