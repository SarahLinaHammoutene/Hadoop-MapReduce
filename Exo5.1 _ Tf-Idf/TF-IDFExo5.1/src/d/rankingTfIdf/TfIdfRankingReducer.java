package d.rankingTfIdf;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import d.rankingTfIdf.TfIdfRankingDriver;

public class TfIdfRankingReducer extends Reducer<DoubleWritable, Text, Text, Text> {
    
	
	private static final DecimalFormat DF = new DecimalFormat("###.########");
	
	// The aim is the last job to re write the values in their correct shape 
	// Output:  <tfidf, word@documentName > in an descending order
	
    @Override
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
			
    	
	    	String tmp = key.toString();
	    	Double tfidf = key.get() ;
	    	tfidf = tfidf / -100000000;	// get the real alue of tfidf
	    	
	    	for (Text value : values) {
	    		context.write( new Text("Term: " + value) , new Text("Tf Idf: "+DF.format(tfidf)));   
	        }
	        	
	         	
	}

    
    
	
}
