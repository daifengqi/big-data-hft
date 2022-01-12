package hook;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;



public class Mapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<Object, Text, Text, DoubleWritable> {

    private final static DoubleWritable one = new DoubleWritable(1);

    public void map(Object key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) {

        try {
            String[] oneRecord = value.toString().split(",");

            String SecurityID = oneRecord[0];

            String TradeTimeRaw = oneRecord[1];
            String TradeTime = TradeTimeRaw.substring(0, 12); // include minute only

            // key
            output.collect(new Text(SecurityID + '#' + TradeTime), one);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
