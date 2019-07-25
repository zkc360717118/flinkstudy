package xuwei.tech.streaming.customPartition;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import xuwei.tech.streaming.custumSource.MyNoParalleSource;

public class SteamingDemoWithMyParitition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> data = env.addSource(new MyNoParalleSource());

        //把数据转换成tuple，才能使用自定义分区
        SingleOutputStreamOperator<Tuple1<Long>> tupleData = data.map(new MapFunction<Long, Tuple1<Long>>() {
            @Override
            public Tuple1<Long> map(Long value) throws Exception {
                return new Tuple1(value);
            }
        });

        //分区之后的数据
        DataStream<Tuple1<Long>> tuple1DataStream = tupleData.partitionCustom(new MyPartition(), 0);
        SingleOutputStreamOperator<Long> result = tuple1DataStream.map(new MapFunction<Tuple1<Long>, Long>() {
            @Override
            public Long map(Tuple1<Long> value) throws Exception {
                System.out.println("当前线程：" + Thread.currentThread().getId() + "数据是：" + value);

                return value.getField(0);
            }
        });

        result.print().setParallelism(1);
        env.execute("test");
    }
}
