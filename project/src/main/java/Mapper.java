// The following is a map process

import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

class Recorder {
    public int mintime;
    public int maxtime;
    public double open;
    public double close;
    public double high;
    public double low;

    Recorder(int mintimeIn, int maxtimeIn, double openIn, double closeIn, double highIn, double lowIn) {
        mintime = mintimeIn;
        maxtime = maxtimeIn;
        open = openIn;
        close = closeIn;
        high = highIn;
        low = lowIn;
    }
}

public class Mapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<Object, Text, Text, DoubleWritable> {

    private final static DoubleWritable one = new DoubleWritable(1);
    /* hashmap */
    private static final HashMap<String, Recorder> map = new HashMap<>();
    /* idea: count how many times each key appears, read into memory as a map.
    *        when count is reached, then collect the data.*/

    public void map(Object key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) {

        try {
            String[] oneRecord = value.toString().split(",");

            // col1: SecurityID
            String SecurityID = oneRecord[0];

            // col2: TradeTime
            String TradeTimeRaw = oneRecord[1];
            String TradeTime = TradeTimeRaw.substring(12); // include minute only

            // col3: TradePrice
            double TradePrice = Double.parseDouble(oneRecord[2]);

            // update price data in hashmap
            if (map.containsKey(TradeTime)) {
                Recorder inner = map.get(TradeTime);
                /* update open price */
                if (Integer.parseInt(TradeTimeRaw) < inner.mintime) {
                    inner.mintime = Integer.parseInt(TradeTimeRaw);
                    inner.open = TradePrice;
                }
                /* update close price */
                if (Integer.parseInt(TradeTimeRaw) > inner.maxtime) {
                    inner.maxtime = Integer.parseInt(TradeTimeRaw);
                    inner.close = TradePrice;
                }
                /* update high price */
                if (TradePrice > inner.high) {
                    inner.high = TradePrice;
                }
                /* update low price */
                if (TradePrice < inner.low) {
                    inner.low = TradePrice;
                }
            } else {
                map.put(TradeTime, new Recorder(Integer.parseInt(TradeTimeRaw),
                        Integer.parseInt(TradeTimeRaw),
                        TradePrice,
                        TradePrice,
                        TradePrice,
                        TradePrice));
            }

            // col4: TradeQty
            double TradeQty = Double.parseDouble(oneRecord[3]);

            // col5：TradeAmount = TradePrice * TradeQty
            double TradeAmount = Double.parseDouble(oneRecord[4]);

            // key
            output.collect(new Text(SecurityID + '#' + TradeTime), one);
            output.collect(new Text(SecurityID + '#' + TradeTime + "#Price"), new DoubleWritable(TradePrice));
            output.collect(new Text(SecurityID + '#' + TradeTime + "#Qty"), new DoubleWritable(TradeQty));
            output.collect(new Text(SecurityID + '#' + TradeTime + "#Amount"), new DoubleWritable(TradeAmount));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
