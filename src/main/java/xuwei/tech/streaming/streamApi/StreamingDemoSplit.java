package xuwei.tech.streaming.streamApi;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import xuwei.tech.streaming.custumSource.MyNoParalleSource;

import java.util.ArrayList;

public class StreamingDemoSplit {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> s1 = env.addSource(new MyNoParalleSource());
        SplitStream<Long> split = s1.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                ArrayList<String> output = new ArrayList<>();
                if (value % 2 == 0) {
                    output.add("even");
                } else {
                    output.add("odd");//奇数
                }
                return output;
            }
        });

        DataStream<Long> even = split.select("even");//选择偶数流
//        split.select("even","odd");//选择偶数流 和奇数流

        even.print().setParallelism(1);

        env.execute("StreamingDemoSplit");
    }
}
