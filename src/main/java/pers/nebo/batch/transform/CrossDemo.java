package pers.nebo.batch.transform;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.operators.DataSource;

import java.util.ArrayList;

/**
 * @ auther fnb
 * @ email nebofeng@gmail.com
 * @ date  2020/5/28
 * @ des :CrossDemo 笛卡尔积
 */
public class CrossDemo {

    public static void main(String[] args) throws  Exception {
        ExecutionEnvironment env=ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> data1 = new ArrayList<>();
        data1.add("java");
        data1.add("flink");
        ArrayList<Integer> data2=new ArrayList<>();
        data2.add(1);
        data2.add(2);


        DataSource<String> text1 = env.fromCollection(data1);

        DataSource<Integer> text2 = env.fromCollection(data2);

        CrossOperator.DefaultCross<String,Integer> cross =text1.cross(text2);

        cross.print();



    }

}
