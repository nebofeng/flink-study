package pers.nebo.streaming.watermark;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import pers.nebo.streaming.watermark.eventtime.WindowWordCountByEventTime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;


/**
 * @ auther fnb
 * @ email nebofeng@gmail.com
 * @ date  2020/8/6
 * @ des :
 * 自定义延迟容忍时间
 * 单词计数： 窗口+水印+延迟容忍时间+多并行度
 *
 */



public class WindowWordCountByWaterMark {
    public static void main(String[] args) throws  Exception{
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(2);
        //设置时间为 事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> dataStream = env.socketTextStream("localhost",8888);

        OutputTag<Tuple2<String,Long>> outputTag =new OutputTag<Tuple2<String, Long>>("late-date"){};

        SingleOutputStreamOperator<String> result =dataStream.map(new MapFunction<String, Tuple2<String,Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                return null;
            }
        }).assignTimestampsAndWatermarks(new EventTimeExtractor())
                .keyBy(0)
                .timeWindow(Time.seconds(3))
                //.allowedLateness(Time.seconds(2)) 允许事件迟到2s
                .sideOutputLateData(outputTag)//保留迟到太多的数据
                .process(new SumProcessWindowFunction());

        //打印结果
        result.print();

        //处理超时的数据
        result.getSideOutput(outputTag).map(new MapFunction<Tuple2<String, Long>, String>() {
            @Override
            public String map(Tuple2<String, Long> value) throws Exception {
                return "超时数据： "+value.toString();
            }
        }).print();

        env.execute("WindowWordCountByWaterMark");


    }

    /**
     * IN, OUT, KEY, W
     * IN：输入的数据类型
     * OUT：输出的数据类型
     * Key：key的数据类型（在Flink里面，String用Tuple表示）
     * W：Window的数据类型
     */
    public  static  class SumProcessWindowFunction extends ProcessWindowFunction<Tuple2<String,Long>,String, Tuple, TimeWindow>{

        FastDateFormat dateFormat=FastDateFormat.getInstance("HH:mm:ss");

        @Override
        public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
            System.out.println("处理时间： "+dateFormat.format(context.currentProcessingTime()));
            System.out.println("window start time" +dateFormat.format(context.window().getStart()));
            List<String> list=new ArrayList<>();
            for(Tuple2<String,Long> ele:elements){
                list.add(ele.toString()+"|"+dateFormat.format(ele.f1));
            }
            out.collect(list.toString());
            System.out.println("window end time : "+dateFormat.format(context.window().getEnd()));
        }

//        @Override
//        public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
//
//        }
    }



    /**
     * 时间水印触发函数
     */
    public  static  class EventTimeExtractor implements AssignerWithPeriodicWatermarks<Tuple2<String,Long>>{
        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");
        private  Long currentMaxEventTime=0L;
        private  Long maxOutOfOrderness=10000L;//最大乱序时间

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxEventTime-maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
            long currentElementTime=element.f1;
            currentMaxEventTime=Math.max(currentElementTime,currentMaxEventTime);
            long id = Thread.currentThread().getId();

            System.out.println("当前线程id="+id+"event = "+element
            +"|"+dateFormat.format(element.f1) // Event Time
            +"|"+dateFormat.format(currentMaxEventTime) //Max Event Time
            +"|"+dateFormat.format(getCurrentWatermark().getTimestamp()) // currentWatemark
            );
            return currentElementTime;
        }
    }


}
