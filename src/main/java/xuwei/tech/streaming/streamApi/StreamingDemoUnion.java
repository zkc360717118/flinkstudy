package xuwei.tech.streaming.streamApi;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import xuwei.tech.streaming.custumSource.MyNoParalleSource;

public class StreamingDemoUnion {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> s1 = env.addSource(new MyNoParalleSource());
        DataStreamSource<Long> s2 = env.addSource(new MyNoParalleSource());
        DataStream<Long> text = s1.union(s2);
        SingleOutputStreamOperator<Long> num = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("原始接收到的数据：" + value);
                return value;
            }
        });

        SingleOutputStreamOperator<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);
        sum.print().setParallelism(1);

        env.execute("StreamingDemoUnion");
    }
}
