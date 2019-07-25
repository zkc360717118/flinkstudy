package xuwei.tech.streaming.custumSource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 自定义实现并行度为1的source
 *
 * 模拟产生从1开始的递增
 数字
 *
 *
 * 注意：
 * SourceFunction 和 SourceContext 都需要指定数据类型，如果不指定，代码运行的时候会报错
 * Caused by: org.apache.flink.api.common.functions.InvalidTypesException:
 * The types of the interface org.apache.flink.streaming.api.functions.source.SourceFunction could not be inferred.
 * Support for synthetic interfaces, lambdas, and generic or raw types is limited at this point
 *
 *
 */
public class MyNoParalleSource implements SourceFunction<Long> {
    private long count = 1L;
    private boolean isRunning = true;
    /**
     * 主要的方法
     * 启动一个source
     * 大部分情况下，都需要在这个run方法中实现一个循环，这样就可以循环产生数据了
     * @param sourceContext
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        while (isRunning) {
            sourceContext.collect(count);
            count++;
            //每秒产生一次数据
            Thread.sleep(1000);
        }
    }

    /**
     * 取消一个cancel时候会调用的方法
     */
    @Override
    public void cancel() {
        isRunning = false;
    }
}
