package pers.nebo.streaming.transformation;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pers.nebo.streaming.source.MyNoParalleSource;

import java.util.ArrayList;

/**
 * @ auther fnb
 * @ email nebofeng@gmail.com
 * @ date  2020/5/26
 * @ des :
 */
public class SplitDemo {

    public static void main(String[] args) throws  Exception {

        //get run env
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

        //get datasource

        DataStreamSource<Long> text=env.addSource(new MyNoParalleSource()).setParallelism(1);

        SplitStream<Long> splitStream =text.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                ArrayList<String> outPut = new ArrayList<>();
                if(value%2==0){
                    outPut.add("even");
                }else{
                    outPut.add("odd");
                }
                return outPut;
            }
        });

        DataStream<Long> oddStream=splitStream.select("odd");
        oddStream.print().setParallelism(1);

       // DataStream<Long> moreStream=splitStream.select("odd","even");

        //sprint result

        //moreStream.print().setParallelism(1);

        String jobName=SplitDemo.class.getSimpleName();
        env.execute(jobName);



    }

}
