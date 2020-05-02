package pers.nebo.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2020/5/2
 * @ des :
 */
public class WordCountDemo {

    public static void main(String[] args) throws  Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname=parameterTool.get("hostname");
        int port = parameterTool.getInt("port");

        //获取执行环境
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据源
        DataStream<String> dataStream=env.socketTextStream(hostname,port);

        //执行逻辑
        DataStream<WordCount> wordAndOneStream=dataStream.flatMap(new FlatMapFunction<String, WordCount>() {
            public void flatMap(String line, Collector<WordCount> cout) throws Exception {
                String[] fields=line.split("\t");
                 for(String word:fields){
                     cout.collect(new WordCount(word,1));
                 }
            }
        });

        DataStream<WordCount> resultStream=wordAndOneStream.keyBy("word")
                .timeWindow(Time.seconds(2),Time.seconds(1))
                .sum("count");

        resultStream.print();

        env.execute("WindowWordCountJava");

    }

    public static class WordCount{
        public String word;
        public long count;
        //记得要有这个空构建
        public WordCount(){

        }
        public WordCount(String word,long count){
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
