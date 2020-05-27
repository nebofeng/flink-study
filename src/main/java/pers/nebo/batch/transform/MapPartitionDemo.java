package pers.nebo.batch.transform;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * @ auther fnb
 * @ email nebofeng@gmail.com
 * @ date  2020/5/26
 * @ des :
 */
public class MapPartitionDemo {

    public static void main(String[] args) throws  Exception {
        //get run env
        ExecutionEnvironment env =ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<String> data=new ArrayList<>();
        data.add("test1 test2");
        data.add("test1 test2");
        data.add("test1 test2");
        DataSource<String>  source =env.fromCollection(data);

        DataSet<String> mapPartitionData=source.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> values, Collector<String> out) throws Exception {
                //get connection
                // values ===data from one partittion
                Iterator<String> it =     values.iterator();
                while(it.hasNext()){
                    String next =it.next();
                    String[] split= next.split("\\W+");
                    for(String word:split){
                        out.collect(word);
                    }
                }
                //close connection
            }
        });
        mapPartitionData.print();
    }
}
