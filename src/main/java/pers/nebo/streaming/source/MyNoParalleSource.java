package pers.nebo.streaming.source;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2020/5/13
 * @ des :
 */

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 注意：指定数据类型
 * 功能：每秒产生一条数据
 */
public class MyNoParalleSource implements SourceFunction<Long> {
    private long number = 1L;
    private boolean isRunning = true;

    public void run(SourceContext<Long> sct) throws Exception {
        while (isRunning){
            sct.collect(number);
            number++;
            //每秒生成一条数据
            Thread.sleep(1000);
        }

    }


    public void cancel() {
        isRunning=false;
    }
}