// The following is a map process

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Mapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<Object, Text, Text, DoubleWritable> {

    private final static DoubleWritable one = new DoubleWritable(1);

    public void map(Object key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {

        try {
            String[] oneRecord = value.toString().split(",");

            // 列1：证券代码
            String SecurityID = oneRecord[0];

            // 列2：成交时间
            String TradeTimeRaw = oneRecord[1];
            String TradeTime = TradeTimeRaw.substring(4, 12); // remove year (2019), include minute; finally 8 digits

            // 列3：成交价格
            double TradePrice = Double.parseDouble(oneRecord[2]);

            // 列4：成交量
            double TradeQty = Double.parseDouble(oneRecord[3]);

            // 列5：成交金额 = TradePrice（成交价格） * TradeQty（成交量）
            double TradeAmount = Double.parseDouble(oneRecord[4]);

            // key：证券代码 + 成交时间 + ""
            output.collect(new Text(SecurityID + '#' + TradeTime), one);
            output.collect(new Text(SecurityID + '#' + TradeTime + "#Price"), new DoubleWritable(TradePrice));
            output.collect(new Text(SecurityID + '#' + TradeTime + "#Qty"), new DoubleWritable(TradeQty));
            output.collect(new Text(SecurityID + '#' + TradeTime + "#Amount"), new DoubleWritable(TradeAmount));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
