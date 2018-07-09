package edu.unc.mapseq.messaging;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

public class Scratch {

    @Test
    public void testModulus() {
        for (int i = 0; i < 186; ++i) {
            if ((i % 4) == 0) {
                System.out.println(i);
            }
        }
    }

    @Test
    public void testSampleSheetParsing() throws Exception {

        File input = new File("/tmp", "180405_UNC31-K00269_0122_AHTV3MBBXX.csv");

        try (FileReader fr = new FileReader(input); BufferedReader br = new BufferedReader(fr)) {

            boolean canStartReading = false;
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("Lane,Sample_ID,Sample_Name,")) {
                    canStartReading = true;
                    continue;
                }

                if (!canStartReading) {
                    continue;
                }

                // Lane,Sample_ID,Sample_Name,Sample_Plate,Sample_Well,I7_Index_ID,index,I5_Index_ID,index2,Sample_Project,Description
                String[] lineSplit = StringUtils.splitPreserveAllTokens(line, ",");
                Integer lane = Integer.valueOf(lineSplit[0]);
                String sampleName = lineSplit[2];
                String barcode = lineSplit[6];
                String sampleProject = lineSplit[9];
                String description = lineSplit[10];
                System.out.println(String.format("%s,%s,%s,%s,%s", lane, sampleName, barcode, sampleProject, description));

            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
