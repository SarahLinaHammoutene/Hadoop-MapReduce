package b.wordCountsForDocs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountsForDocsReducer extends Reducer<Text, Text, Text, Text> {
 
    public WordCountsForDocsReducer() {
    }
 
    /*	
    	   Receive a list of <"documentName", ["word1=3", "word2=5", "word3=5"]>
           Output: <"word@documentName, NbOfOccurence/SumOfWordsInFile">
     */
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sumOfWordsInDocument = 0;
        Map<String, Integer> Counter = new HashMap<String, Integer>();
        
        for (Text val : values) {
            String[] wordCounter = val.toString().split("=");
            Counter.put(wordCounter[0], Integer.valueOf(wordCounter[1]));
            sumOfWordsInDocument += Integer.parseInt(val.toString().split("=")[1]);
        }
       
        for (String wordKey : Counter.keySet()) {
            context.write(new Text(wordKey + "@" + key.toString()), new Text(Counter.get(wordKey) + "/"
                    + sumOfWordsInDocument));
        }
    }
}