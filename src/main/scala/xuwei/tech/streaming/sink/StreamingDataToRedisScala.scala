package xuwei.tech.streaming.sink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object StreamingDataToRedisScala {
  def main(args: Array[String]): Unit = {
    //隐式转换
    import org.apache.flink.api.scala._

    //获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //配置source  用socket
    val text = env.socketTextStream("192.168.43.90",9000,'\n')

    //把数据转换成tuple
    val l_wordsScalaData = text.map(line=>("l_wordsScalaData",line))

    //配置redis连接池
    val conf = new FlinkJedisPoolConfig.Builder().setHost("192.168.43.90").setPort(6379).build()

    //配置RedisSink
    val redisSink = new RedisSink[Tuple2[String,String]](conf,new MyRedisMapper())

    //添加sink
    l_wordsScalaData.addSink(redisSink)
    //执行任务
    env.execute("redis scala sink")
  }
}

class MyRedisMapper extends RedisMapper[Tuple2[String,String]]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.LPUSH)
  }

  override def getKeyFromData(t: (String, String)): String = {
    t._1
  }

  override def getValueFromData(t: (String, String)): String = {
    t._2
  }
}