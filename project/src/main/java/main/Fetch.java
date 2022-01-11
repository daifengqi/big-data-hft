package main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/**
 * Copy one single .csv file to preview.
 */
public class Fetch {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            conf.addResource(new Path("../resources/core-site.xml"));

            // HFDS file path
            Path path = new Path("/Group2/tickDataOutput");
            // local file path
            Path targetPath = new Path("../tickDataOutput");

            FileSystem fs = path.getFileSystem(conf);
            fs.copyToLocalFile(path, targetPath);

            fs.close();
            System.out.println("----------------");
            System.out.println("🎉 MapReduce result is copied to local path.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
