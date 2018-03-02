package cafe.hr20.algorithm.imputation;

public class ImputationTechniqueApplication {
    public static void main(String[] args) {
        
        String outputCsvFileFormat_01 = "missing01_imputed";
        String outputCsvFileFormat_10 = "missing10_imputed";
        
        String dataSetMissing_01 = "dataset_missing01.csv"; 
        String dataSetMissing_10 = "dataset_missing10.csv";
        String dataSetComplete = "dataset_complete.csv";
        
        System.out.println("MAE_01_mean : " + 
                ImputationTechnique.performMeanImputation(dataSetMissing_01, outputCsvFileFormat_01, dataSetComplete));
        
        System.out.println("MAE_01_mean_conditional : " + 
                ImputationTechnique.performConditionalMeanImputation(dataSetMissing_01, outputCsvFileFormat_01, dataSetComplete));
        
        System.out.println("MAE_01_hd : " +
                ImputationTechnique.performHotDeckImputation_v2(dataSetMissing_01, outputCsvFileFormat_01, dataSetComplete));
        
        System.out.println("MAE_01_hd_conditional: " +
                ImputationTechnique.conditionalHotDeckImputation(dataSetMissing_01, outputCsvFileFormat_01, dataSetComplete));
        
        
        System.out.println("MAE_10_mean : " + 
                ImputationTechnique.performMeanImputation(dataSetMissing_10, outputCsvFileFormat_10, dataSetComplete));
        
        System.out.println("MAE_10_mean_conditional : " + 
                ImputationTechnique.performConditionalMeanImputation(dataSetMissing_10, outputCsvFileFormat_10, dataSetComplete));
    
        System.out.println("MAE_10_hd : " +
                ImputationTechnique.performHotDeckImputation_v2(dataSetMissing_10, outputCsvFileFormat_10, dataSetComplete));
    
        System.out.println("MAE_10_hd_conditional: " +
                ImputationTechnique.conditionalHotDeckImputation(dataSetMissing_10, outputCsvFileFormat_10, dataSetComplete));   
    }
}
