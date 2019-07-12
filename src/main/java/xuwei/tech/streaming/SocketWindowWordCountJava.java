package xuwei.tech.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 滑动窗口计算
 *
 * 通过socket模拟产生单词数据
 * flink对数据进行统计计算
 *
 * 需要实现每隔1秒对最近2秒内的数据进行汇总计算
 *
 *
 * Created by xuwei.tech on 2018/10/8.
 */
public class SocketWindowWordCountJava {
    public static void main(String[] args) throws Exception {
        //获取需要的端口号
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("not  port set,will use default port:9000");
            port = 9000;
        }

        //获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String hostname = "linux01";
        String delimiter = "\n";

        //连接socket获取输入数据
        DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter);

        // 数据样本 a b c a
        //          -> a 1
        //          -> b 1
        //          -> c 1
        //          -> a 1

        SingleOutputStreamOperator<WordWithCount> windowCount = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String input, Collector<WordWithCount> output) throws Exception {
                String[] splits = input.split("\\s");
                for (String word : splits) {
                    output.collect(new WordWithCount(word, 1));
                }
            }
        }).keyBy("word")
                .timeWindow(Time.seconds(2), Time.seconds(1)) //每隔一秒，计算时间窗口为2秒的数据
//                .sum("count"); // 也可以使用reduce
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
                        return new WordWithCount(a.word, a.count + b.count);
                    }
                });

        //把数据打印到控制台
        windowCount.print().setParallelism(1);

        //执行
        env.execute("count example start");
    }

    public static class WordWithCount{
        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
