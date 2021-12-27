import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Reducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, IntWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        // 计算总数
        double sum = 0;
        double numCity = 12;
        double result;

        while (values.hasNext()) {
            IntWritable value = values.next();
            sum += value.get();
        }

        // 对于不同的key进行计数: Q1的key中含有数字，Q2的key中不含数字
        if (Character.isDigit(key.toString().charAt(0))) {
            // 对于Q1：除以city的数量得到ratio
            result = sum / numCity;
        } else {
            // 对于Q2，直接就是计数的sum
            result = sum;
        }

        output.collect(key, new DoubleWritable(result));
    }
}
