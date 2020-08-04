package pers.nebo.streaming.state.operatorstate;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @ auther fnb
 * @ email nebofeng@gmail.com
 * @ date  2020/6/15
 * @ des :
 *
 * checkpoint --> 保存
 */
public class CustomSink implements SinkFunction<Tuple2<String,Integer>>, CheckpointedFunction {

    //用于缓存结果数据的
    private List<Tuple2<String,Integer>> bufferElements ;

    //表示内存中数据的大小阈值
    private  int threshold;

    //用户保存内存中的状态问题
    private ListState<Tuple2<String ,Integer>> checkpointState;

    //StateBackend
    //checkpoint

    public CustomSink(int threshold){
        this.threshold=threshold;
        this.bufferElements=new ArrayList<>();
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        bufferElements.add(value);
        if(bufferElements.size()==threshold){
            System.out.println("自定义格式： "+bufferElements);
            bufferElements.clear();
        }

    }

    // 用于将内存中数据保存到状态中
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointState.clear();
        for(Tuple2<String,Integer> ele:bufferElements){
            checkpointState.add(ele);
        }
    }


    // 用于在程序恢
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        ListStateDescriptor<Tuple2<String,Integer>> descriptor =new ListStateDescriptor<Tuple2<String, Integer>>(
                "bufferd-elements",
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>(){}));

          //注册一个 operator state
        checkpointState = context.getOperatorStateStore().getListState(descriptor);
        if(context.isRestored()){
            for(Tuple2<String,Integer> ele: checkpointState.get()){
                //内存
                bufferElements.add(ele);
            }
        }

    }
}
