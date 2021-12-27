import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class read {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            conf.addResource(new Path("../resources/core-site.xml"));

            Path path = new Path("/file_8MB.txt");
            FileSystem fs = path.getFileSystem(conf);
            FileStatus status = fs.getFileStatus(path);

            long fileSize = status.getLen();
            System.out.println("(1). The size of the file: " + fileSize /(1024 * 1024) + "mb");

            long blockSize = status.getBlockSize();
            long blockNumToStore = fileSize / blockSize + 1;
            System.out.println("(2). We need " + blockNumToStore + " blocks to store the file.");

            BlockLocation[] blockLocations = fs.getFileBlockLocations(path, 0, fileSize);
            System.out.println("(3). The locations of the blocks are: ");
            System.out.println(blockLocations[0].toString());
            System.out.println(blockLocations[1].toString());

            fs.close();
            System.out.println("----------------");
            System.out.println("Question 1 done!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
