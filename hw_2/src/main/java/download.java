import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.core.ZipFile;


public class download {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            conf.addResource(new Path("D:/Software/hadoop-3.1.4/etc/hadoop/core-site.xml"));

            Path path = new Path("/airQuality.zip");
            Path targetPath = new Path("data/airQuality.zip");
            String destination = "data/airQuality";
            FileSystem fs = path.getFileSystem(conf);

            fs.copyToLocalFile(path, targetPath);
            ZipFile zipFile = new ZipFile("data/airQuality.zip");
            zipFile.extractAll(destination);
            File[] fileListTmp = new File(destination).listFiles();
            File[] fileList = fileListTmp[0].listFiles();
            for (File file : fileList){
                System.out.println("----------------");
                System.out.println("file name is:");
                System.out.println(file.getName());
                System.out.println("file path is:");
                System.out.println(file.toString());
                System.out.println("first 5 lines:");
                BufferedReader br = new BufferedReader(new FileReader(file.toString()));
                for (int i=0; i<=5; i++){
                    System.out.println(br.readLine());
                }
            }
            fs.close();
            System.out.println("----------------");
            System.out.println("Question 3 done!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
