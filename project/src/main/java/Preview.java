import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/**
 * Copy one single .csv file to preview.
 */
public class Preview {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            conf.addResource(new Path("../resources/core-site.xml"));

            // HFDS file path
            Path path = new Path("/tickData/tickData/201901/20190102.csv");
            // local file path
            Path targetPath = new Path("src/main/tickData/201901/20190102.csv");

            FileSystem fs = path.getFileSystem(conf);
            fs.copyToLocalFile(path, targetPath);

            fs.close();
            System.out.println("----------------");
            System.out.println("🎉 Csv file is copied to local path.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
