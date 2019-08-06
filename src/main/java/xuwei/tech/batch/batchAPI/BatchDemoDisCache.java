package xuwei.tech.batch.batchAPI;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Distributed Cache
 *
 *
 *
 * Created by xuwei.tech on 2018/10/8.
 */
public class BatchDemoDisCache {
    public static void main(String[] args) throws Exception {

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //注册一个文件，可以使用hdfshuoz s3文件
        env.registerCachedFile("d:\\data\\a.txt","a.txt");

        DataSource<String> data = env.fromElements("a", "b", "c", "d");
        MapOperator<String, String> map = data.map(new RichMapFunction<String, String>() {
            private ArrayList<String> dataList = new ArrayList<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //使用文件
                File myFile = getRuntimeContext().getDistributedCache().getFile("a.txt");
                List<String> lines = FileUtils.readLines(myFile);
                for (String line : lines) {
                    dataList.add(line);
                    System.out.println("data:  " + line);
                }
            }

            @Override
            public String map(String value) throws Exception {
                //由于open初始化的时候 获取了缓存的数据，接下来在map中就可以使用
                return value;
            }
        });

        map.print();
    }
}
