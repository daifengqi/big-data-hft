// The following is a map process

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class Mapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        // 数据预处理：如果是表头（key是0），不处理；
        // 如果PM2.5的值是Nan，不处理；
        if (key.toString().equals("0"))
            return;
        String[] oneRecord = value.toString().split(",");

        if (oneRecord[5].equals(("NA")))
            return;

        // For Q1：记录output的key：“年_月_日_小时_good(medium/bad)”
        String time = String.format("%s_%s_%s_%s", oneRecord[1], oneRecord[2], oneRecord[3], oneRecord[4]);
        String station = oneRecord[17];
        Double PM25 = Double.valueOf(oneRecord[5]);
        Text goodTime = new Text(time + "_good");
        Text mediumTime = new Text(time + "_medium");
        Text badTime = new Text(time + "_bad");

        // For Q2：记录output的key，“station_fine”
        Text stationFine = new Text(station + "_fine");
        Text stationBad = new Text(station + "_bad");

        // 对PM25的值进行判断，将对应Q1 Q2的output写入context中
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
