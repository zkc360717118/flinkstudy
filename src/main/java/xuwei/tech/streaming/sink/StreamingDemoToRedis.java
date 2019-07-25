package xuwei.tech.streaming.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * 接收socket数据，把数据保存到redis中
 *
 * list
 *
 * lpush list_key value
 *
 */
public class StreamingDemoToRedis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.socketTextStream("192.168.43.90", 9000, "\n");

        //转换数据成tuple2<String,String>
        SingleOutputStreamOperator<Tuple2<String, String>> l_words = text.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return new Tuple2("l_words", value);  // "l_words"作为列表的键名字;
            }
        });

        //创建redis配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("192.168.43.90").setPort(6379).build();
        //创建redisSink
        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(conf, new MyRedisMapper());
        //添加sink
        l_words.addSink(redisSink);
        //开始执行
        env.execute("redisSinkTest");
    }

    public static class MyRedisMapper implements RedisMapper<Tuple2<String,String>>{
        //指定redis操作命令
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }

        //表示从接收的数据中获取需要操作的redis value
        @Override
        public String getKeyFromData(Tuple2<String, String> stringStringTuple2) {
            return stringStringTuple2.f0;
        }
        //表示从接收的数据中获取需要操作的redis key

        @Override
        public String getValueFromData(Tuple2<String, String> stringStringTuple2) {
            return stringStringTuple2.f1;
        }
    }

}
