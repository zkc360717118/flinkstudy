package xuwei.tech.batch.batchAPI;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class BatchDemoDistinct {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> data = new ArrayList<>();
        data.add("hello you");
        data.add("hello me");

        DataSource<String> text = env.fromCollection(data);
        FlatMapOperator<String, String> flatWords = text.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split("\\W+");
                for (String word : split) {
                    out.collect(word);
                }
            }
        });

        flatWords.distinct().print();

    }
}
