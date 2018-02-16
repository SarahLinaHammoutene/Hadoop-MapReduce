package d.rankingTfIdf;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TfIdfRankingMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        // The aim of the last job is to rank the result in a in descending order
    	// The idea is to use the ranking of Hadoop 
    	// Since the data are ordered is an ascending order, we define the value (td idf) as the key of the pair and then 
    	// multiplie it  by -1 in order to get the right order 
    	
    	//      Output: < -(tfidf), word@documentName > 
    	
    	//Get the value of tf idf
        String tmp= "TF-IDF: ";
        String val = value.toString() ;        

    	int idx1 = value.find("TF-IDF: ");
        int idx2 = value.getLength() ;
        String s= val.substring(idx1+tmp.length(), idx2);
        
        Double tfidf= Double.parseDouble(s);
        tfidf =tfidf  *  -1 ;			// To reverse the order
        int idx3= value.find("	 ");
        String termdoc= val.substring(0, idx3);
        
        
		context.write(new DoubleWritable(tfidf), new Text(termdoc));


        
    }
       

}
