package c.wordsInCorpusTFIDF;


import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WordsInCorpusTFIDFReducer extends Reducer<Text, Text, Text, Text> {

   private static final DecimalFormat DF = new DecimalFormat("###.########");

   public WordsInCorpusTFIDFReducer() {
   }


    //   Receive a list of <word, ["docName1=n1/N1", "docName1=n2/N2"]>
    //   Output : <"word@docName1,  [d/D, n/N, TF-IDF]">
    
   protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    
	   // Get the number of documents from the file-system that are stored in the job name
       int numberOfDocumentsInCorpus = Integer.parseInt(context.getJobName());
      
       // Total frequency of the word
       int numberOfDocumentsInCorpusWhereWordAppears = 0;
       Map<String, String> tempFrequencies = new HashMap<String, String>();
       
       for (Text val : values) {
           String[] documentAndFrequencies = val.toString().split("=");
           numberOfDocumentsInCorpusWhereWordAppears++;
           tempFrequencies.put(documentAndFrequencies[0], documentAndFrequencies[1]);
       }
       
       for (String document : tempFrequencies.keySet()) {
           String[] wordFrequenceAndTotalWords = tempFrequencies.get(document).split("/");

           //Term frequency = the number of terms in document / the total number of terms in the doc
           
           double tf = Double.valueOf(Double.valueOf(wordFrequenceAndTotalWords[0])
                   / Double.valueOf(wordFrequenceAndTotalWords[1]));

           //Interse document frequency quotient between the number of docs in corpus and number of docs the term appears
           double idf = (double) numberOfDocumentsInCorpus / (double) numberOfDocumentsInCorpusWhereWordAppears;

           //The term frequency in documents
           double tfIdf = numberOfDocumentsInCorpus == numberOfDocumentsInCorpusWhereWordAppears ?
                   tf : tf * Math.log10(idf);
           
           //We multiply it to avoid overflow when we multiply it by -1 in the next job (ranking)
           tfIdf = tfIdf * 100000000;     
           
           context.write(new Text(key + "@" + document), new Text(" Term frequency in the corpus: " + numberOfDocumentsInCorpusWhereWordAppears + "/"
                   + numberOfDocumentsInCorpus + " , Term frequency in the document: " + wordFrequenceAndTotalWords[0] + "/"
                   + wordFrequenceAndTotalWords[1] + " , TF-IDF: " + DF.format(tfIdf)));
       }
   }
}