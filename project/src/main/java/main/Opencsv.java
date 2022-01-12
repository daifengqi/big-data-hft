package main;

import com.opencsv.CSVReader;

import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class Opencsv {


    public static void readDataLineByLine(String file) {

        try {

            FileReader filereader = new FileReader(file);

            CSVReader csvReader = new CSVReader(filereader, ' ');
            String[] nextRecord;

            while ((nextRecord = csvReader.readNext()) != null) {
                boolean flag = true;
                String key = "foo";
                double value = 1;
                for (String cell : nextRecord) {
                    if (flag) {
                        key = cell;
                    } else {
                        value = Double.parseDouble(cell);
                    }
                    flag = !flag;
                    if (!flag) {
                        Driver.keyCountMap.put(key, value);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Opencsv.readDataLineByLine("src/main/resources/test.csv");
    }

}
