package pers.nebo.streaming.state.operatorstate;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ auther fnb
 * @ email nebofeng@gmail.com
 * @ date  2020/6/15
 * @ des :
 */
public class TestOperatorStateMain {

    public static void main(String[] args) throws  Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String,Integer>> dataStreamSource =env.fromElements(Tuple2.of("Spark", 3), Tuple2.of("Hadoop", 5), Tuple2.of("Hadoop", 7),
                Tuple2.of("Spark", 4));


        dataStreamSource.addSink(new CustomSink(1)).setParallelism(1);

        env.execute("TestStatefulApi");



    }
}
