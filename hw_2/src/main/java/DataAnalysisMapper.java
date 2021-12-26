// The following is a map process

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;


public class DataAnalysisMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, OutputCollector<Text, IntWritable> output) throws IOException, InterruptedException {
        // 数据预处理：如果是表头（key是0），不处理；如果PM2.5的值是Nan，不处理；直接返回
        if (key.toString().equals("0"))
            return;
        String[] oneRecord = value.toString().split(",");

        if (oneRecord[5].equals(("NA")))
            return;

        // For Q1：记录output的key：“年_月_日_小时_good(medium/bad)”
        String time = String.format("%s_%s_%s_%s", oneRecord[1].toString(), oneRecord[2].toString(), oneRecord[3].toString(), oneRecord[4].toString());
        String station = oneRecord[17];
        Double PM25 = Double.valueOf(oneRecord[5].toString());
        Text goodTime = new Text(time + "_good");
        Text mediumTime = new Text(time + "_medium");
        Text badTime = new Text(time + "_bad");

        //For Q2：记录output的key，“station_fine”
        Text stationFine = new Text(station + "_fine");
        Text stationBad = new Text(station + "_bad");

        //对雨PM25的值进行判断，将对应Q1 Q2的output写入context中
        if (PM25 <= 35) {
            output.collect(goodTime, one);
            output.collect(stationFine, one);
        } else if (PM25 <= 75) {
            output.collect(mediumTime, one);
            output.collect(stationFine, one);
        } else {
            output.collect(badTime, one);
            output.collect(stationBad, one);
        }
    }
}
