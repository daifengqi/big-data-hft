import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

public class DataAnalysisReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    public void reduce(Text key, Iterable<IntWritable> values, OutputCollector<Text, Double> output) throws IOException {
        // 计算总数
        double sum = 0;
        double numCity = 12;
        double result;


        for (IntWritable value: values){
            sum += value.get();
        }
        // 对于不同的key进行计数: Q1的key中含有数字，Q2的key中不含数字
        if (Character.isDigit(key.toString().charAt(0))){
            // 对于Q1：除以city的数量得到ratio
            result = sum / numCity;
            }
        else{
            // 对于Q2，直接就是计数的sum
            result = sum;
        }
        output.collect(key, result);
    }
}
