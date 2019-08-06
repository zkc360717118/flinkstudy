package xuwei.tech.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**
 * WINDOW的增量操作
 */
public class SocketDemoFullCount {
    public static void main(String[] args) throws Exception {
        //获取端口号
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("No port set. use default port 9000--java");
            port = 9000;
        }

        //获取flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //连接socket获取输入的数据
        DataStreamSource<String> text = env.socketTextStream("192.168.43.90", port, "\n");

        //转换数据为int类型
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> intData = text.map(new MapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(String value) throws Exception {
                return new Tuple2<>(1, Integer.parseInt(value));
            }
        });

        //window的增量操作
        intData.keyBy(0)//按照tuple的第一个元素分组
                .timeWindow(Time.seconds(5)) //每五秒为一个滚动窗口
                .process(new ProcessWindowFunction<Tuple2<Integer,Integer>, String, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple key, Context context, Iterable<Tuple2<Integer, Integer>> elements, Collector<String> out) throws Exception {
                        System.out.println("执行process");
                        long count=0;
                        for (Tuple2<Integer, Integer> element : elements) {
                            count++;
                        }
                        out.collect("WINDOW:"+context.window()+",count:"+count);
                    }
                }).print();

        //这一行代码一定要实现，否则程序不执行
        env.execute("Socket window count");
        //3> WINDOW:TimeWindow{start=1565068630000, end=1565068635000},count:4
    }
}
