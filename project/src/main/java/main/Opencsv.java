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
                for (String cell : nextRecord) {
                    System.out.println("cell" + cell);
                    String key = cell.substring(0, 19);
                    String value = cell.substring(20);
                    System.out.println("key" + key);
                    System.out.println("value" + value);
                    Driver.keyCountMap.put(key, Double.parseDouble(value));
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
