package pers.nebo.streaming.sink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pers.nebo.streaming.source.MyNoParalleSource;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2020/5/13
 * @ des :
 */
public class WriteTextDemo {

    public static void main(String[] args) {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long>  numStream=env.addSource(new MyNoParalleSource()).setParallelism(1);

        SingleOutputStreamOperator<Long> dataStream =numStream.map(new MapFunction<Long, Long>() {
            public Long map(Long value) throws Exception {

                System.out.println("接收到了数据： "+value);
                return value;
            }
        });

        SingleOutputStreamOperator<Long>  filterDataStream=dataStream.filter(new FilterFunction<Long>() {
            public boolean filter(Long aLong) throws Exception {
                return aLong  %2==0;
            }
        });

        //设置并行度为 1 ,输出到一个文件里面
        filterDataStream.writeAsText("./test").setParallelism(1);
        try {
            env.execute("WriteTextDemo");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}
