package xuwei.tech.streaming.streamApi;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import xuwei.tech.streaming.custumSource.MyNoParalleSource;

public class StreamingDemoFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> s1 = env.addSource(new MyNoParalleSource());

        SingleOutputStreamOperator<Long> filterData = s1.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {

                return value % 2 == 0; //只返回偶数
            }
        });


        filterData.print().setParallelism(1); // 返回 2 4 6 8 10

        env.execute("StreamingDemoFilter");
    }
}
