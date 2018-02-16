package job3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class PageRankJob3Mapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        /* Rank Ordering
         * We map the noderank (key) to its value (node), then Hadoop sorting them in an ascending order
         * Same as for tfIdf we multiply the noderang by -1 to reverse the order.
         */
        
        int tIdx1 = value.find("\t");
        int tIdx2 = value.find("\t", tIdx1 + 1);
        
        // extract tokens 
        String node = Text.decode(value.getBytes(), 0, tIdx1);
        float nodeRank = Float.parseFloat(Text.decode(value.getBytes(), tIdx1 + 1, tIdx2 - (tIdx1 + 1))) * (-1);
        
        context.write(new DoubleWritable(nodeRank), new Text(node));
        
    }
       

}
