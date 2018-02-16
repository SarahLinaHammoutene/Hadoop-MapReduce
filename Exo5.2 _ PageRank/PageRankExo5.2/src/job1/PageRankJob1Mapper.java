package job1;

import java.io.IOException;

import job3.PageRankJob3Driver;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import param.PageRankParam;

public class PageRankJob1Mapper  extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        // Parse a line of the input graph creating a map with key-values pairs.
    	// output <Node A , Node B>
	
    	//We skip the first lines 
        
        if (value.charAt(0) != '#') {
            
            int tabIndex = value.find("\t");
            String nodeA = Text.decode(value.getBytes(), 0, tabIndex);
            String nodeB = Text.decode(value.getBytes(), tabIndex + 1, value.getLength() - (tabIndex + 1));
            context.write(new Text(nodeA), new Text(nodeB));
            
            // add the current source node to the node list so we can compute the total amount of nodes of our graph in Job#2
            PageRankParam.NODES.add(nodeA);
            // also add the target node to the same list
            PageRankParam.NODES.add(nodeB);
            
        }
 
    }

}
