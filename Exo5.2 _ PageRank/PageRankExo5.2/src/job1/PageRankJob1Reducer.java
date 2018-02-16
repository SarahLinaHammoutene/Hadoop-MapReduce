package job1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import job3.PageRankJob3Driver;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import param.PageRankParam;

public class PageRankJob1Reducer extends Reducer<Text, Text, Text, Text> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        //  Output:   <Node>    <page-rank>    <link1>,<link2>,<link3>,<link4>,...,<linkN>
        
    	//WE set the initial page rank value to 1    		
    
        boolean first = true;
        String s= "";

        String links = "";
        for (Text value : values) {
        	if (value.toString().compareTo("")!=0) {
        		if (first) links = "1\t" + value.toString() ; 
        		else 
        			links =  links + "," + value.toString();			//We seperate the links by a comma
        		first = false ;
        	}
        }
      
       
        context.write(key, new Text(links));
    }
    

}
