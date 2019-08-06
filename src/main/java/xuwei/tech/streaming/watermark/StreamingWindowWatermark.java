package xuwei.tech.streaming.watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
 * 案例
 */
public class StreamingWindowWatermark {
    public static void main(String[] args) throws Exception {
        //定义socket的端口号
        int port = 9000;
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置使用eventtime，默认是使用processtime
        DataStreamSource<String> text = env.socketTextStream("192.168.43.90", port, "\n");

        //转化数据
        SingleOutputStreamOperator<Tuple2<String, Long>> inputMap = text.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
            }
        });

        //抽取timestamp和生成watermark
        SingleOutputStreamOperator<Tuple2<String, Long>> waterMarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
            Long currentMaxTimestamp = 0L;
            final long maxOutOfOrderness = 10000L; //允许最大的乱序时间 10s
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            /**
             * 定义生成watermark的逻辑
             * 默认100ms被调用一次
             * @return
             */
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            //定义如何提取timstamp
            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
                long timestamp = element.f1;
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);//赋值目前最大的时间戳
                long id = Thread.currentThread().getId();
                System.out.println("currentThreadId:" + id + ",key:" + element.f0 + ",eventtime:[" + element.f1 + "|" + sdf.format(element.f1) + "],currentMaxTimestamp:[" + currentMaxTimestamp + "|" +
                        sdf.format(currentMaxTimestamp) + "],watermark:[" + getCurrentWatermark().getTimestamp() + "|" + sdf.format(getCurrentWatermark().getTimestamp()) + "]");
                return timestamp;
            }
        });

        SingleOutputStreamOperator<String> window = waterMarkStream.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3))) //按照消息的EventTime分配窗口，和调用给的TimeWindow一样
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    /**
                     * 对window内的数据进行排序，保证数据的顺序
                     * @param tuple
                     * @param window
                     * @param input
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                        String key = tuple.toString();
                        //把数据放到arrayList里面的
                        ArrayList<Long> arrayList = new ArrayList<>();
                        Iterator<Tuple2<String, Long>> it = input.iterator();
                        while (it.hasNext()) {
                            Tuple2<String, Long> next = it.next();
                            arrayList.add(next.f1);
                        }
                        Collections.sort(arrayList);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        String result = key + "," + arrayList.size() + "," + sdf.format(arrayList.get(0)) + "," + sdf.format(arrayList.get(arrayList.size() - 1))
                                + "," + sdf.format(window.getStart()) + "," + sdf.format(window.getEnd());
                        out.collect(result);

                    }
                });
        //测试把结果打印到控制台即可
        window.print();

        //注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        env.execute("eventtime-watermark");

    }
}
