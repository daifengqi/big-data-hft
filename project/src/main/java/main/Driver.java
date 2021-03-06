package main;// The following is the driver

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.util.HashMap;

public class Driver {

    public static final HashMap<String, Double> keyCountMap = new HashMap<>();
    public static String file = "20190102.csv";

    public static void init() {
        main.Opencsv.readDataLineByLine("src/main/resources/" + file);
    }

    public static void main(String[] args) {

        // set the map
        Driver.init();

        JobClient myClient = new JobClient();
        // Create a configuration object for the job
        JobConf jobConf = new JobConf(java.sql.Driver.class);
        // Set a name of the Job
        jobConf.setJobName("Big_Data_Project");
        // Specify map type
        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(DoubleWritable.class);
        // Specify data type of output key and value
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(DoubleWritable.class);
        // Specify names of main.Mapper and main.Reducer Class
        jobConf.setMapperClass(Mapper.class);
        jobConf.setReducerClass(Reducer.class);
        // Specify formats of the data type of Input and output
        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        // set input and output
        FileInputFormat.setInputPaths(jobConf, hook.Driver.inputPath);
        FileOutputFormat.setOutputPath(jobConf, new Path("/Group2/tickDataOutput/" + file));

        myClient.setConf(jobConf);
        try {
            // Run the job
            JobClient.runJob(jobConf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
