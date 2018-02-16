package job3;

import java.io.IOException;

import job3.PageRankJob3Driver;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import param.PageRankParam;

public class PageRankJob3Reducer extends Reducer<DoubleWritable, Text, Text, Text> {
    
	
	//We reverse the order by re multiplying the value of the page rank by -1
	// output < Page > < PageRank> 
    @Override
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

    	     String tmp = key.toString();
	    	Double pageRank = key.get() ;
	    	pageRank = pageRank * -1;	
	    	
	    	for (Text value : values) {
	    		context.write( new Text("Page: " + value) , new Text("Page Rank: "+pageRank));   
	        }
	        	
	         	
	}

    
    
	
}
