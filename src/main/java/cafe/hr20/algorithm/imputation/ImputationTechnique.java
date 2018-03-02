package cafe.hr20.algorithm.imputation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ImputationTechnique {
    public static synchronized String performMeanImputation(String missingDatasetInputFile, String theOutputFile, String completeDatasetFile) {
        Map<Integer, List<String>> dataSet = Utility.convertCsvToDataSet_v2(missingDatasetInputFile);
        Set<Integer> keys = dataSet.keySet();
        for(Integer key : keys) {
            if(!dataSet.get(key).get(0).trim().equalsIgnoreCase("class")) {
                List<String> featureDataSet = dataSet.get(key);
                double sum = 0.0;
                int countNonEmpty = 0;
                for(int i = 1 ; i < featureDataSet.size() ; i++) {
                    String featureData = featureDataSet.get(i);
                    if(!featureData.trim().equalsIgnoreCase("?") ) {
                        sum = sum + Double.parseDouble(featureData);
                        countNonEmpty++;
                    }
                }
                
                double meanValue = sum / countNonEmpty;
                meanValue = Double.parseDouble(Utility.imputedValueFormatter.format(meanValue));
                for(int i = 0 ; i < featureDataSet.size() ; i++) {
                    String data = featureDataSet.get(i);
                    if(data.trim().equalsIgnoreCase("?")) {
                        featureDataSet.set(i, String.valueOf(meanValue));
                    }
                }
                dataSet.put(key, featureDataSet);
            }
        }
        Utility.writeOutputCsv_v2(dataSet, theOutputFile + "_mean.csv");
        Map<String, List<Double>> calculatedErrors = calculateErrorForEachColumn(missingDatasetInputFile, completeDatasetFile, dataSet);
        Set<String> columnNames = calculatedErrors.keySet();
        for(String column : columnNames) {
            List<Double> calcluatedErrorsForColumn = calculatedErrors.get(column);
            List<Double> absCalcluatedErrors = new ArrayList<Double>(calcluatedErrorsForColumn);
            for(int i = 0 ; i < calcluatedErrorsForColumn.size() ; i++) {
                double absErrorValue = Math.abs(calcluatedErrorsForColumn.get(i));
                absCalcluatedErrors.remove(i);
                absCalcluatedErrors.add(i, absErrorValue);
            }
            calculatedErrors.put(column, absCalcluatedErrors);
        }
        double calculatedMae = Utility.calculateMaeValues(calculatedErrors);
        return "" + Utility.calculatedErrorValueFormatter.format(calculatedMae); 
    }
    
    public static synchronized String performConditionalMeanImputation(String missingDatasetInputFile, String theOutputFile, String completeDatasetFile) {
        Map<Integer, List<String>> dataSet = Utility.convertCsvToDataSet_v2(missingDatasetInputFile);
        Set<Integer> keys = dataSet.keySet();
        for(Integer key : keys) {
            List<Double> nonEmptyYValues = new ArrayList<Double>();
            List<Double> nonEmptyNValues = new ArrayList<Double>();
            if(!dataSet.get(key).get(0).trim().equalsIgnoreCase("class")) {
                List<String> featureDataSet = dataSet.get(key);
                for(int i = 1 ; i < featureDataSet.size() ; i++) {
                    String featureData = featureDataSet.get(i);
                    if(!featureData.trim().equalsIgnoreCase("?")) {
                        if(dataSet.get(53).get(i).equalsIgnoreCase("Y")) {
                            nonEmptyYValues.add(Double.parseDouble(featureData));
                        }
                        if(dataSet.get(53).get(i).equalsIgnoreCase("N")) {
                            nonEmptyNValues.add(Double.parseDouble(featureData));
                        }
                    }
                }
                double sumYValues = 0.0;
                for(Double yValue : nonEmptyYValues) {
                    sumYValues = sumYValues + yValue;
                }
                double meanYValue = (sumYValues / nonEmptyYValues.size());
                double sumNValues = 0.0;
                for(Double nValue : nonEmptyNValues) {
                    sumNValues = sumNValues + nValue;
                }
                double meanNValue = (sumNValues / nonEmptyNValues.size());
                
                for(int i = 0 ; i < featureDataSet.size() ; i++) {
                    String data = featureDataSet.get(i);
                    
                    if(data.trim().equalsIgnoreCase("?")) {
                        if(dataSet.get(53).get(i).equalsIgnoreCase("Y")) {
                            featureDataSet.set(i, Utility.imputedValueFormatter.format(meanYValue));
                        }
                        if(dataSet.get(53).get(i).equalsIgnoreCase("N")) {
                            featureDataSet.set(i, Utility.imputedValueFormatter.format(meanNValue));
                        }
                    }
                }
                dataSet.put(key, featureDataSet);
            }
        }
        Utility.writeOutputCsv_v2(dataSet, theOutputFile + "_mean_conditional.csv");
        Map<String, List<Double>> calculatedErrors = calculateErrorForEachColumn(missingDatasetInputFile, completeDatasetFile, dataSet);
        Set<String> columnNames = calculatedErrors.keySet();
        for(String column : columnNames) {
            List<Double> calcluatedErrorsForColumn = calculatedErrors.get(column);
            List<Double> absCalcluatedErrors = new ArrayList<Double>(calcluatedErrorsForColumn);
            for(int i = 0 ; i < calcluatedErrorsForColumn.size() ; i++) {
                double absErrorValue = Math.abs(calcluatedErrorsForColumn.get(i));
                absCalcluatedErrors.remove(i);
                absCalcluatedErrors.add(i, absErrorValue);
            }
            calculatedErrors.put(column, absCalcluatedErrors);
        }
        double calculatedMae = Utility.calculateMaeValues(calculatedErrors);
        return "" + Utility.calculatedErrorValueFormatter.format(calculatedMae); 
    }
    
    public static Map<String, List<Double>> calculateErrorForEachColumn(String missingDataFileName, String completeDataFileName, Map<Integer, List<String>> imputedDataSet) {
        Map<String, List<Double>> calculatedErrors = new HashMap<String, List<Double>>();
        Map<Integer, List<String>> missingDataSet = Utility.convertCsvToDataSet_v2(missingDataFileName);
        Map<Integer, List<String>> completeDataSet = Utility.convertCsvToDataSet_v2(completeDataFileName);
        
        Set<Integer> missingDataKeySet = missingDataSet.keySet();
        List<Integer> missingDataKeyList = new ArrayList<Integer>(missingDataKeySet);
        Set<Integer> imputedDataKeySet = imputedDataSet.keySet();
        List<Integer> imputedDataKeyList = new ArrayList<Integer>(imputedDataKeySet);
        Set<Integer> completeDataKeySet = completeDataSet.keySet();
        List<Integer> completeDataKeyList = new ArrayList<Integer>(completeDataKeySet);
        
        if(missingDataKeyList == null || missingDataKeyList.size() < 2) {
            System.out.println("WARN: Missing Dataset is found kinda empty while calculating MAE.");
            return null;
        }
        if(imputedDataKeyList == null || imputedDataKeyList.size() < 2) {
            System.out.println("WARN: Imputed Dataset is found kinda empty while calculating MAE.");
            return null;
        }
        if(completeDataKeyList == null || completeDataKeyList.size() < 2) {
            System.out.println("WARN: Complete Dataset is found kinda empty while calculating MAE.");
            return null;
        }
        Collections.sort(missingDataKeyList);
        Collections.sort(imputedDataKeyList);
        Collections.sort(completeDataKeyList);
        try {
            Iterator<Integer> missingDataColumnIterator = missingDataKeyList.iterator();
            Iterator<Integer> imputedDataColumnIterator = imputedDataKeyList.iterator();
            Iterator<Integer> completeDataColumnItertor = completeDataKeyList.iterator();
            
            while(missingDataColumnIterator.hasNext() && imputedDataColumnIterator.hasNext() 
                && completeDataColumnItertor.hasNext()) {                
                int columnNameIndex = 1;
                List<String> missingFeatureData = missingDataSet.get(missingDataColumnIterator.next());
                List<String> imputedFeatureData = imputedDataSet.get(imputedDataColumnIterator.next());
                List<String> completeFeatureData = completeDataSet.get(completeDataColumnItertor.next());
                Iterator<String> featureDataIterator = missingFeatureData.iterator();
                Iterator<String> imputedDataIterator = imputedFeatureData.iterator();
                Iterator<String> completeDataIterator = completeFeatureData.iterator();
                List<Double> calculatedErrorList = new ArrayList<Double>();
                String columnName = "";
                
                while(featureDataIterator.hasNext() && imputedDataIterator.hasNext() 
                    && completeDataIterator.hasNext()) {
                    String value = featureDataIterator.next();
                    if(columnNameIndex == 1) {
                        calculatedErrors.put(value, calculatedErrorList);
                        columnName = value;
                    }
                    if(value.trim().equals("?")) {
                        String imputedValue = imputedDataIterator.next();
                        String actualValue = completeDataIterator.next();
                        double calculatedError = (Double.parseDouble(imputedValue) - Double.parseDouble(actualValue));
                        calculatedErrorList.add(calculatedError);
                    }
                    else {
                        imputedDataIterator.next();
                        completeDataIterator.next();
                    }
                    calculatedErrors.put(columnName, calculatedErrorList);
                    columnNameIndex++;
                }
            } 
        }
        catch(Exception ex) {
            System.out.println("Error in Calculating MAE values. : " + ex);
            return null;
        }
        return calculatedErrors;
    }
    
    public static synchronized String performHotDeckImputation_v2(String missingDatasetInputFile, String theOutputFile, String completeDatasetFile) {
        Map<Integer, List<String>> dataSet = Utility.convertCsvToDataSet_v2(missingDatasetInputFile);
        
        int executionCount = 0;
        Map<Integer, List<Double>> euclideanDistances = new HashMap<Integer, List<Double>>();
        List<Double> euclideanDistancesList = new ArrayList<Double>();
        Set<Integer> keys = dataSet.keySet();
        for(Integer key : keys) {
            if(!dataSet.get(key).get(0).trim().equalsIgnoreCase("class")) {
                List<String> featureDataSet = dataSet.get(key);
                
                int rowCount = 1;
                for(; rowCount < featureDataSet.size(); rowCount++) {
                    euclideanDistancesList = euclideanDistances.get(rowCount);
                    if(euclideanDistancesList == null)
                        euclideanDistancesList = new ArrayList<Double>();
                    
                    /**
                     * Here, we have list of records for each Column. 
                     * 
                     * Calculate the Distance of each row from every other row, square the distance and put it as an entry in a List.
                     */
                    double xDiff = 0.0;
                    double xSqrDiff = 0.0;
                    
                    for(int j = 1; j < featureDataSet.size(); j++) {
                        if(j == rowCount) {
                            xDiff = 0.0;
                        }
                        else if(featureDataSet.get(rowCount).trim().equals("?") || featureDataSet.get(j).trim().equals("?"))
                            xDiff = 1.0;
                        else
                            xDiff = Double.parseDouble(featureDataSet.get(rowCount)) - Double.parseDouble(featureDataSet.get(j));
                        
                        xSqrDiff = Math.pow(xDiff, 2);
                        String xSqrDiffStr = Utility.imputedValueFormatter.format(xSqrDiff);
                        double euclideanDistance = Double.parseDouble(xSqrDiffStr);
                        if(executionCount == 0) {
                            euclideanDistancesList.add(euclideanDistance);
                        }
                        else {
                            euclideanDistance = (euclideanDistancesList.get(rowCount-1) + euclideanDistance);
                            euclideanDistance = Double.parseDouble(Utility.imputedValueFormatter.format(euclideanDistance));
                            euclideanDistancesList.set(rowCount-1, euclideanDistance);
                        }
                    }
                    List<Double> euclideanDistancesSqrtRootList = new ArrayList<Double>();
                    for(Double eachDistance : euclideanDistancesList) {
                        euclideanDistancesSqrtRootList.add(Math.sqrt(eachDistance));
                    }
                    euclideanDistances.put(rowCount, euclideanDistancesList);
                }
                executionCount++;
            }
        }
        Collections.sort(euclideanDistancesList);
        
        /**
         * perform the adding imputed values logic here
         */
        Set<Integer> keySet = dataSet.keySet();
        for(Integer key : keySet) {
            if(!dataSet.get(key).get(0).trim().equalsIgnoreCase("class")) {
                List<String> featureDataSet = dataSet.get(key);
                int rowNeeded = 0;
                for(int i = 1 ; i < featureDataSet.size() ; i++) {
                    if(featureDataSet.get(i).trim().equalsIgnoreCase("?") ) {
                        rowNeeded = i; 
                    }
                    if(rowNeeded > 0) {
                        List<Double> calculatedDistances = euclideanDistances.get(rowNeeded);
                        
                        int maxIterationForMissingValue = 3;
                        int minIndex = findMinDistanceValueIndex(calculatedDistances, featureDataSet, maxIterationForMissingValue);
                        featureDataSet.set(rowNeeded, featureDataSet.get(minIndex));
                        rowNeeded = 0;
                        dataSet.put(key, featureDataSet);
                    }
                }
            }
        }
        Utility.writeOutputCsv_v2(dataSet, theOutputFile + "_hd.csv");
        Map<String, List<Double>> calculatedErrors = calculateErrorForEachColumn(missingDatasetInputFile, completeDatasetFile, dataSet);
        
        for(String column : calculatedErrors.keySet()) {
            List<Double> calcluatedErrorsForColumn = calculatedErrors.get(column);
            List<Double> absCalcluatedErrors = new ArrayList<Double>(calcluatedErrorsForColumn);
            
            for(int i = 0 ; i < calcluatedErrorsForColumn.size() ; i++) {
                double absErrorValue = Math.abs(calcluatedErrorsForColumn.get(i));
                absCalcluatedErrors.remove(i);
                absCalcluatedErrors.add(i, absErrorValue);
            }
            calculatedErrors.put(column, absCalcluatedErrors);
        }
        double calculatedMae = Utility.calculateMaeValues(calculatedErrors);
        return "" + Utility.calculatedErrorValueFormatter.format(calculatedMae); 
    }
    
    public static int findMinDistanceValueIndex(List<Double> calculatedDistances, List<String> featureDataSet, int maxIterationForMissingValue) {
        int minIndex = calculatedDistances.indexOf(Collections.min(calculatedDistances)) + 1;
        while(featureDataSet.get(minIndex).trim().equalsIgnoreCase("?") && maxIterationForMissingValue > 0) {
            if(maxIterationForMissingValue == 0) {
                throw new RuntimeException("Data is incorrect. ? found while calculating missing value from dataset. Reference DataSet: " + featureDataSet.get(0));
            }
            List<Double> copied = new ArrayList<Double>();
            for(int index = 0 ; index < calculatedDistances.size() ; index++) {
                copied.add(calculatedDistances.get(index));
            }
            minIndex = copied.indexOf(Collections.min(copied)) + 1;
            --maxIterationForMissingValue;
        }
        if(featureDataSet.get(minIndex).trim().equalsIgnoreCase("?")) {
            //System.out.println("? found in nearest possible value. Replacing verdict with 1st element.");
            return 1;
        }
        return minIndex;
    }
    
    public static int findConditionalMinDistanceIndex(List<Double> calculatedDistances, Map<Integer, List<String>> dataSet, int column, int row, int maxIterationForMissingValue) {
        int minIndex = 0;
        List<String> classDataSet = dataSet.get(53);
        List<KeyValPair> keyValuePairsY = new ArrayList<KeyValPair>();
        List<KeyValPair> keyValuePairsN = new ArrayList<KeyValPair>();
        
        for(int i = 1; i < classDataSet.size(); i++) {
            if(classDataSet.get(i).trim().equalsIgnoreCase("Y")) {
                keyValuePairsY.add(new KeyValPair(i, calculatedDistances.get(i-1)));
            }
            else {
                keyValuePairsN.add(new KeyValPair(i, calculatedDistances.get(i-1)));
            }
        }
        String classValue = dataSet.get(53).get(row);
        Collections.sort(keyValuePairsY);
        Collections.sort(keyValuePairsN);
        int nextCount = 0;
        if(classValue == "Y") {
            minIndex = keyValuePairsY.get(nextCount).key;
        }
        else {
            minIndex = keyValuePairsN.get(nextCount).key;
        }
        
        List<String> featureDataSet = dataSet.get(column);
        while(featureDataSet.get(minIndex).trim().equalsIgnoreCase("?") && maxIterationForMissingValue > 0) {
            if(maxIterationForMissingValue == 0) {
                throw new RuntimeException("Data is incorrect. ? found while calculating missing value from dataset. Reference DataSet: " + featureDataSet.get(0));
            }
            List<Double> copied = new ArrayList<Double>();
            
            for(int index = 0; index < calculatedDistances.size(); index++) {
                copied.add(calculatedDistances.get(index));
            }
            if(classValue.equalsIgnoreCase("Y")) {
                minIndex = keyValuePairsY.get(nextCount++).key;
            }
            else {
                minIndex = keyValuePairsN.get(nextCount++).key;
            }
            --maxIterationForMissingValue;
        }
        if(featureDataSet.get(minIndex).trim().equalsIgnoreCase("?")) {
            System.out.println("? found in nearest possible value. Replacing verdict with 1st element.");
            return 1;
        }
        return minIndex;
    }
    
    public static synchronized String conditionalHotDeckImputation(String missingDatasetInputFile, String theOutputFile, String completeDatasetFile) {
        Map<Integer, List<String>> dataSet = Utility.convertCsvToDataSet_v2(missingDatasetInputFile);
        
        int executionCount = 0;
        Map<Integer, List<Double>> euclideanDistances = new HashMap<Integer, List<Double>>();
        List<Double> euclideanDistancesList = new ArrayList<Double>();
        Set<Integer> keys = dataSet.keySet();
        for(Integer key : keys) {
            if(!dataSet.get(key).get(0).trim().equalsIgnoreCase("class")) {
                List<String> featureDataSet = dataSet.get(key);
                
                int rowCount = 1;
                for(; rowCount < featureDataSet.size(); rowCount++) {
                    euclideanDistancesList = euclideanDistances.get(rowCount);
                    if(euclideanDistancesList == null)
                        euclideanDistancesList = new ArrayList<Double>();
                    
                    /**
                     * Here, we have list of records for each Column. 
                     * 
                     * Calculate the Distance of each row from every other row, square the distance and put it as an entry in a List.
                     */
                    double xDiff = 0.0;
                    double xSqrDiff = 0.0;
                    
                    for(int j = 1; j < featureDataSet.size(); j++) {
                        if(j == rowCount) {
                            xDiff = 0.0;
                        }
                        else if(featureDataSet.get(rowCount).trim().equals("?") || featureDataSet.get(j).trim().equals("?"))
                            xDiff = 1.0;
                        else
                            xDiff = Double.parseDouble(featureDataSet.get(rowCount)) - Double.parseDouble(featureDataSet.get(j));
                        
                        xSqrDiff = Math.pow(xDiff, 2);
                        String xSqrDiffStr = Utility.imputedValueFormatter.format(xSqrDiff);
                        double euclideanDistance = Double.parseDouble(xSqrDiffStr);
                        if(executionCount == 0) {
                            euclideanDistancesList.add(euclideanDistance);
                        }
                        else {
                            euclideanDistance = (euclideanDistancesList.get(rowCount-1) + euclideanDistance);
                            euclideanDistance = Double.parseDouble(Utility.imputedValueFormatter.format(euclideanDistance));
                            euclideanDistancesList.set(rowCount-1, euclideanDistance);
                        }
                    }
                    List<Double> euclideanDistancesSqrtRootList = new ArrayList<Double>();
                    for(Double eachDistance : euclideanDistancesList) {
                        euclideanDistancesSqrtRootList.add(Math.sqrt(eachDistance));
                    }
                    euclideanDistances.put(rowCount, euclideanDistancesList);
                }
                executionCount++;
            }
        }
        Collections.sort(euclideanDistancesList);
        
        /**
         * perform the adding imputed values logic here
         */
        Set<Integer> keySet = dataSet.keySet();
        for(Integer key : keySet) {
            if(!dataSet.get(key).get(0).trim().equalsIgnoreCase("class")) {
                List<String> featureDataSet = dataSet.get(key);
                int rowNeeded = 0;
                for(int i = 1 ; i < featureDataSet.size() ; i++) {
                    if(featureDataSet.get(i).trim().equalsIgnoreCase("?") ) {
                        rowNeeded = i; 
                    }
                    if(rowNeeded > 0) {
                        List<Double> calculatedDistances = euclideanDistances.get(rowNeeded);
                        int maxIterationForMissingValue = 3;
                        int minIndex = findConditionalMinDistanceIndex(calculatedDistances, dataSet, key, rowNeeded, maxIterationForMissingValue);
                        featureDataSet.set(rowNeeded, featureDataSet.get(minIndex));
                        rowNeeded = 0;
                        dataSet.put(key, featureDataSet);
                    }
                }
            }
        }
        Utility.writeOutputCsv_v2(dataSet, theOutputFile + "_hd_conditional.csv");
        
        Map<String, List<Double>> calculatedErrors = calculateErrorForEachColumn(missingDatasetInputFile, completeDatasetFile, dataSet);
        for(String column : calculatedErrors.keySet()) {
            List<Double> calcluatedErrorsForColumn = calculatedErrors.get(column);
            List<Double> absCalcluatedErrors = new ArrayList<Double>(calcluatedErrorsForColumn);
            for(int i = 0 ; i < calcluatedErrorsForColumn.size() ; i++) {
                double absErrorValue = Math.abs(calcluatedErrorsForColumn.get(i));
                absCalcluatedErrors.remove(i);
                absCalcluatedErrors.add(i, absErrorValue);
            }
            calculatedErrors.put(column, absCalcluatedErrors);
        }
        double calculatedMae = Utility.calculateMaeValues(calculatedErrors);
        return "" + Utility.calculatedErrorValueFormatter.format(calculatedMae);
    }
}


class Utility {

    public static final DecimalFormat imputedValueFormatter = new DecimalFormat("#.#####");
    public static final DecimalFormat calculatedErrorValueFormatter = new DecimalFormat("#.####");
    
    public static Map<Integer, List<String>> convertCsvToDataSet_v2(String csvFileName) {
        if(csvFileName == null || csvFileName.trim().length() < 5) {
            System.out.println("Given dataset file is not valid.");
            return null;
        }
        
        ClassLoader classLoader = new Utility().getClass().getClassLoader();
        File csvFile = new File(classLoader.getResource(csvFileName).getFile());
        
        Map<Integer, List<String>> dataSet = new HashMap<Integer, List<String>>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            String line;
            for(int width = 0, arbitraryColumnCount = 40; width < 14; width ++) {
                List<String> dataList = new ArrayList<String>();
                dataSet.put(arbitraryColumnCount++, dataList);
            }
            while((line = br.readLine()) != null) {
                int columnCount = 40;
                
                String[] rowData = line.split(",");
                for(String cell : rowData) {
                    dataSet.get(columnCount).add(cell);
                    columnCount++;
                }
            }
            br.close();
        } catch (Exception exc) {
            System.out.println("Exception happened in converting to an excel file, " + exc);
            return null;
        }
        return dataSet;
    }
    public static double calculateMaeValues(Map<String, List<Double>> absCalculatedErrors) {
        if(absCalculatedErrors != null) {
            int missingElementCount = 0;
            double sum = 0.0;
            for(String column : absCalculatedErrors.keySet()) {
                List<Double> calculatedErrorList = absCalculatedErrors.get(column);
                
                if(calculatedErrorList != null) {
                    for(int i = 0 ; i < calculatedErrorList.size() ; i++) {
                        sum = sum + calculatedErrorList.get(i);
                        missingElementCount++;
                    }
                }
            }
            if(sum != 0.0) {
                return (sum / missingElementCount);
            }
        }
        return 0.0;
    }
    
    public static boolean writeOutputCsv_v2(Map<Integer, List<String>> output, String outputFileName) {
        Set<Integer> keySet = output.keySet();
        List<Integer> keyList = new ArrayList<Integer>(keySet);
        
        if(keyList == null || keyList.size() < 2) {
            System.out.println("WARN: output to write is less than 1 record. Check Input.");
            return false;
        }
        Collections.sort(keyList);
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter("src/main/resources/"+outputFileName));
            int count = 0;
            int dataSetRecordCount = output.get(keyList.get(count)).size();
            
            if(dataSetRecordCount < 2) {
                System.out.println("WARN: output to write is less than 1 record. Check Mate.");
                bw.close();
                return false;
            }
            
            while(count < dataSetRecordCount) {
                StringBuilder sb = new StringBuilder();
                for(Integer key : keyList) {
                    List<String> featureList = output.get(key);
                    sb = sb.append(featureList.get(count)).append(",");
                }
                bw.write(sb.substring(0, sb.length()-1));
                bw.newLine();
                count++;
            }
            bw.close();
        }
        catch (IOException e) {
            throw new RuntimeException("Exception in writing processed data to output file." + e);
        }
        return true;
    }
}

