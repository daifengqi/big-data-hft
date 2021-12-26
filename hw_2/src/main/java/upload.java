import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class upload {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            conf.addResource(new Path("../resources/core-site.xml"));


            String localDir = "D:\\github代码\\big-data-hft\\hw_2\\data/Group2_fileSize_6MB/file.pdf";
            String hdfsDir = "Group2_fileSize_6.6MB";

            //upload file
            Path localPath = new Path(localDir);
            Path hdfsPath = new Path(hdfsDir);
            FileSystem hdfs = FileSystem.get(conf);
            hdfs.copyFromLocalFile(localPath, hdfsPath);

            //write information
            Date date = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String createdate = sdf.format(date);
            FileSystem fs = FileSystem.get(conf);
            byte[] buff = ("Zijie Wang \nYemeng Jie\nLiwen Pang\nDaifeng Qi\nQiang Yin\nXinran Guo\n"+createdate).getBytes();
            String filename = "/Group2_fileSize_6.6MB/head.txt";
            FSDataOutputStream os = fs.append(new Path(filename));
            os.write(buff,0,buff.length);
            System.out.println("Creat:"+ hdfsDir);
            System.out.println("----------------");
            System.out.println("Question 3 done!");
            os.close();
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
