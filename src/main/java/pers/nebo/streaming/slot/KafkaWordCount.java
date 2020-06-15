package pers.nebo.streaming.slot;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @ auther fnb
 * @ email nebofeng@gmail.com
 * @ date  2020/6/5
 * @ des :
 */
public class KafkaWordCount {
    public static void main(String[] args)  throws  Exception{
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        String topic = "testSlot";
        Properties consumerProperties=new Properties();
        consumerProperties.setProperty("bootstrap.servers","192.168.192.1:9092");
        consumerProperties.setProperty("group.id","testSlot_consumer");

        FlinkKafkaConsumer<String> flinkConsumer = new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),consumerProperties);
        DataStreamSource<String> data =  env.addSource(flinkConsumer).setParallelism(3);
        SingleOutputStreamOperator<Tuple2<String,Integer>> wordOneStream=data.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fields = value.split(",");
                for(String word:fields){
                    out.collect(Tuple2.of(word,1));
                }
            }
        }).setParallelism(2);
        SingleOutputStreamOperator<Tuple2<String,Integer>> result = wordOneStream.keyBy(0).sum(1).setParallelism(2);
        result.map(value->value.toString()).setParallelism(2).print().setParallelism(1);
        env.execute("WordCount2");


    }
}
