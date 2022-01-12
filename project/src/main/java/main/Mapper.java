package main;// The following is a map process

import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

class Recorder {
    public double mintime;
    public double maxtime;
    public double open;
    public double close;
    public double high;
    public double low;
    public int count;

    Recorder(double mintimeIn, double maxtimeIn, double openIn, double closeIn, double highIn, double lowIn) {
        this.mintime = mintimeIn;
        this.maxtime = maxtimeIn;
        this.open = openIn;
        this.close = closeIn;
        this.high = highIn;
        this.low = lowIn;
        this.count = 1;
    }
}

public class Mapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<Object, Text, Text, DoubleWritable> {

    private final static DoubleWritable one = new DoubleWritable(1);
    public final static String OpenIdentifier = new String("$Open");
    public final static String CloseIdentifier = new String("$Close");
    public final static String HighIdentifier = new String("$High");
    public final static String LowIdentifier = new String("$Low");

    public final static String QtyIdentifier = new String("$Qty");
    public final static String AmountIdentifier = new String("$Amount");
    public final static String CountIdentifier = new String("$Count");

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
            String TradeTime = TradeTimeRaw.substring(0, 12); // include minute only

            // col3: TradePrice
            double TradePrice = Double.parseDouble(oneRecord[2]);

            String mapKey = SecurityID + '#' + TradeTime;

            // update price data in hashmap
            if (map.containsKey(mapKey)) {

                Recorder inner = map.get(mapKey);
                inner.count = inner.count + 1;

                /* update open price */
                if (Double.parseDouble(TradeTimeRaw) < inner.mintime) {
                    inner.mintime = Double.parseDouble(TradeTimeRaw);
                    inner.open = TradePrice;
                }
                /* update close price */
                if (Double.parseDouble(TradeTimeRaw) > inner.maxtime) {
                    inner.maxtime = Double.parseDouble(TradeTimeRaw);
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
                map.put(mapKey, new Recorder(Double.parseDouble(TradeTimeRaw),
                        Double.parseDouble(TradeTimeRaw),
                        TradePrice,
                        TradePrice,
                        TradePrice,
                        TradePrice));
            }

            // col4: TradeQty
            double TradeQty = Double.parseDouble(oneRecord[3]);

            // col5：TradeAmount = TradePrice * TradeQty
            double TradeAmount = Double.parseDouble(oneRecord[4]);

            if (map.containsKey(mapKey)) {
                Recorder inner = map.get(mapKey);
                if (inner.count == Driver.keyCountMap.get(mapKey)) {
                    output.collect(new Text(SecurityID + '#' + TradeTime + OpenIdentifier), new DoubleWritable(inner.open));
                    output.collect(new Text(SecurityID + '#' + TradeTime + CloseIdentifier), new DoubleWritable(inner.close));
                    output.collect(new Text(SecurityID + '#' + TradeTime + LowIdentifier), new DoubleWritable(inner.low));
                    output.collect(new Text(SecurityID + '#' + TradeTime + HighIdentifier), new DoubleWritable(inner.high));
                }
            }

            output.collect(new Text(SecurityID + '#' + TradeTime + CountIdentifier), one);
            output.collect(new Text(SecurityID + '#' + TradeTime + QtyIdentifier), new DoubleWritable(TradeQty));
            output.collect(new Text(SecurityID + '#' + TradeTime + AmountIdentifier), new DoubleWritable(TradeAmount));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
