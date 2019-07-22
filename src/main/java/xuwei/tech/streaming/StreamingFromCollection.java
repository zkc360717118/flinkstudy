package xuwei.tech.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class StreamingFromCollection {
    public static void main(String[] args) throws Exception {
        //伪造数据
        ArrayList<Integer> data = new ArrayList<Integer>();
        data.add(10);
        data.add(15);
        data.add(20);

        //获取flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //指定数据源
        DataStreamSource<Integer> collection = env.fromCollection(data);
        //规定数据的算子
        SingleOutputStreamOperator<Integer> num = collection.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value + 1;
            }
        });
        //触发计算
        num.print().setParallelism(1);
        env.execute("从collection获取数据计算");
    }
}
