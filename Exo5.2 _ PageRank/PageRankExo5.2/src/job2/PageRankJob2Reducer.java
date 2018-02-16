package job2;

import java.io.IOException;

import job3.PageRankJob3Driver;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import param.PageRankParam;

public class PageRankJob2Reducer extends Reducer<Text, Text, Text, Text> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 
                                                                                InterruptedException {
        

    	// Output: < PageRank>      <link1,link2,link3, ... , linkN>
        
        String links = "";
        double sumPageRanks = 0.0;
        
        for (Text value : values) {
 
            String content = value.toString();
            
            if (content.startsWith(PageRankParam.LINKS_SEPARATOR)) {
            	// If the value contains the node links, we save the links in order to be reused by the mapper for the next iteration 
      
                links += content.substring(PageRankParam.LINKS_SEPARATOR.length());
            } 
            else {
        
            	String[] split = content.split("\t");
          	
                // extract tokens
                    
            	double pageRank = Double.parseDouble(split[0]);
                int totalLinks = Integer.parseInt(split[1]);
               
                // We add the contribution of all the pages having an outlink pointing  to the current node: 
                sumPageRanks += (pageRank / totalLinks);
             
             
            	
            }

        }
        
        // We compute the page rank using the formula
        double newRank = PageRankParam.DAMPING * sumPageRanks + (1 - PageRankParam.DAMPING);
        context.write(key, new Text(newRank + "\t" + links));
        
        
        
    }

}
