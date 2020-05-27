package pers.nebo.batch.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @ auther fnb
 * @ email nebofeng@gmail.com
 * @ date  2020/5/27
 * @ des : 去重算子
 */
public class DistinctDemo {
    public static void main(String[] args)  throws  Exception {
        // get env
        ExecutionEnvironment env =ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> data= new ArrayList<>();

        data.add("we,should");
        data.add("study,eachday");

        DataSource<String> dataSource = env.fromCollection(data);

        DataSet<String> dataSet =dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] fields=s.split(",");
                for(String word : fields){
                    collector.collect(word);
                }
            }
        });

        dataSet.distinct().print();




    }
}
