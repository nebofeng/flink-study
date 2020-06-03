package pers.nebo.batch.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ auther fnb
 * @ email nebofeng@gmail.com
 * @ date  2020/6/2
 * @ des :
 */
public class WordCount {
    public static void main(String[] args) throws  Exception {
        Configuration conf = new Configuration();
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        conf.setInteger(RestOptions.PORT, 8089);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(8, conf);

        DataStreamSource<String> dataStream=env.socketTextStream("192.168.192.1",8888);

        SingleOutputStreamOperator<Tuple2<String,Long>> wordOneStream=dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] fields=value.split(",");
                for(String word:fields){
                    out.collect(Tuple2.of(word,1L));
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String,Long>> result=wordOneStream.keyBy(0).sum(1);
        result.print();

        env.execute(WordCount.class.getSimpleName());

    }


}
