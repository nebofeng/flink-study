package pers.nebo.streaming.watermark.eventtime;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

/**
 * @ auther fnb
 * @ email nebofeng@gmail.com
 * @ date  2020/7/9
 * @ des :
 */
public class WindowWordCountByEventTime {
    public static void main(String[] args)  throws  Exception{
        //get env
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //set eventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> dataStream=env.addSource(new TestSource());

        dataStream.map(new MapFunction<String, Tuple2<String,Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] fields = value.split(",");
                 if(fields.length<2){
                     return  null;
                 }
                return new Tuple2<>(fields[0],Long.valueOf(fields[1]));
            }

            //get eventTime
        }).assignTimestampsAndWatermarks(new EventTimeExtrator());




    }


    public static  class TestSource implements SourceFunction<String>{

        FastDateFormat dateFormat =FastDateFormat.getInstance("HH:mm:ss");
        @Override
        public void run(SourceContext<String> ctx) throws Exception {

            String currTime=String.valueOf(System.currentTimeMillis()) ;
            while (Integer.valueOf(currTime.substring(currTime.length()-4))>100) {
                currTime = String.valueOf(System.currentTimeMillis());
                continue;
            }
            System.out.println("开始发送事件的时间："+dateFormat.format(System.currentTimeMillis()));
            //第13秒 发送两个事件
            TimeUnit.SECONDS.sleep(13);

            ctx.collect("hadoop,"+System.currentTimeMillis());
            //产生了一个时间因为网络原因 时间没有发哦少年宫
            String event="hadoop,"+System.currentTimeMillis();
            //第 16 秒 发送一个事件
            TimeUnit.SECONDS.sleep(3);
            ctx.collect("hadoop,"+System.currentTimeMillis());
            //第19秒的时候发送
            TimeUnit.SECONDS.sleep(3);
            ctx.collect(event);

            TimeUnit.SECONDS.sleep(300);


        }
        @Override
        public void cancel() {

        }
    }




    /**
     * IN, OUT, KEY, W
     * IN：输入的数据类型
     * OUT：输出的数据类型
     * Key：key的数据类型（在Flink里面，String用Tuple表示）
     * W：Window的数据类型
     */
    public static  class SumProcessWindowFunction extends ProcessWindowFunction<Tuple2<String,Long>,Tuple2<String,Integer>, Tuple, TimeWindow>{

        FastDateFormat dateFormat =FastDateFormat.getInstance("HH:mm:ss");

        /**
         * 当一个window触发计算的时候会调用这个方法
         * @param tuple key
         * @param context operator的上下文
         * @param elements 指定window的所有元素
         * @param out 用户输出
         */
        @Override
        public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {

        }
    }


    public  static  class EventTimeExtrator implements AssignerWithPeriodicWatermarks<Tuple2<String,Long>>{

        FastDateFormat fastDateFormat=FastDateFormat.getInstance("HH:mm:ss");


        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return null;
        }

        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
            return element.f1;
        }
    }
}
