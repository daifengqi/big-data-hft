import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Reducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        double count = 0;
        double high = -1;
        double low = 1e9;
        double vwap = 0;
        double amount = 0;

        if (key.toString().endsWith("#Price")) {
            while (values.hasNext()) {
                DoubleWritable value = values.next();
                double val = Double.parseDouble(String.valueOf(value));
                if (val > high) {
                    high = val;
                }
                if (val < low) {
                    low = val;
                }
            }
            output.collect(new Text(key.toString() + "-High"), new DoubleWritable(high));
            output.collect(new Text(key.toString() + "-Low"), new DoubleWritable(low));
        } else if (key.toString().endsWith("#Qty")) {
            while (values.hasNext()) {
                DoubleWritable value = values.next();
                double val = Double.parseDouble(String.valueOf(value));
                vwap += val;
            }
            output.collect(new Text(key.toString()), new DoubleWritable(vwap));
        } else if (key.toString().endsWith("#Amount")) {
            while (values.hasNext()) {
                DoubleWritable value = values.next();
                double val = Double.parseDouble(String.valueOf(value));
                amount += val;
            }
            output.collect(new Text(key.toString()), new DoubleWritable(amount));
        } else {
            while (values.hasNext()) {
                DoubleWritable _value = values.next();
                count += 1;
            }
            output.collect(new Text(key.toString() + "#Count"), new DoubleWritable(count));
        }
    }
}
