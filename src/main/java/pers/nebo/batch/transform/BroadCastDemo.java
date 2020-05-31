package pers.nebo.batch.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ auther fnb
 * @ email nebofeng@gmail.com
 * @ date  2020/5/22
 * @ des :
 */
public class BroadCastDemo {

    public static void main(String[] args)  throws  Exception {
        ExecutionEnvironment env=ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<String, Integer>> broadData = new ArrayList<>();

        broadData.add(new Tuple2<>("zs",18));
        broadData.add(new Tuple2<>("ls",20));
        broadData.add(new Tuple2<>("ww",17));

        DataSet<Tuple2<String, Integer>> tupleData = env.fromCollection(broadData);


        DataSet<HashMap<String,Integer>> toBroadcast  = tupleData.map(new MapFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> map(Tuple2<String, Integer> source) throws Exception {
                HashMap<String, Integer> sourceData = new HashMap<>();
                sourceData.put(source.f0,source.f1);
                return (HashMap<String, Integer>) sourceData;
            }
        });

        DataSource<String> data= env.fromElements("zs", "ls", "ww");

        DataSet<String> result=data.map(new RichMapFunction<String, String>() {
            List<HashMap<String,Integer>> broadCastMap=new ArrayList<>();
            HashMap<String,Integer> allMap=new HashMap<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                this.broadCastMap=getRuntimeContext().getBroadcastVariable("broadCastMapName");
                for(HashMap map:broadCastMap){
                    allMap.putAll(map);
                }
            }

            @Override
            public String map(String value) throws Exception {
                Integer age=allMap.get(value);
                return value+","+age;

            }
        }).withBroadcastSet(toBroadcast,"broadCastMapName");

        result.print();



    }
}
