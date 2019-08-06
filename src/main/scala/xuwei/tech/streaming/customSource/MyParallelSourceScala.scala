package xuwei.tech.streaming.customSource

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

class MyParallelSourceScala  extends ParallelSourceFunction[Long]{
  var count = 1L
  var isRunning = true

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while(isRunning) {
      ctx.collect(count)
      count+=1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
