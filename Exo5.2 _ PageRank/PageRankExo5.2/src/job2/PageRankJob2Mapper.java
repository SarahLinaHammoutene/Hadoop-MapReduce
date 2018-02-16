package job2;

import java.io.IOException;

import job3.PageRankJob3Driver;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import param.PageRankParam;


public class PageRankJob2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        /* Computation of the pageRank 
         * 
         * Output, 2 kinds of records:
         * 1st kind is composed by the collection of links of each page:    
         *     <Node>   |<link1,link2,link3, ... , linkN>
         *     
         * 2nd kind is composed by the linked page, the page rank of the source page and the total amount of out links of the source page:
         *     <Node>    <page-rank>    <total-links>
         */
        
        int index1 = value.find("\t");
        int index2 = value.find("\t", index1 + 1);
        
        // extract tokens from the current line
        String page = Text.decode(value.getBytes(), 0, index1);
        String pageRank = Text.decode(value.getBytes(), index1 + 1, index2 - (index1 + 1));
        String links = Text.decode(value.getBytes(), index2 + 1, value.getLength() - (index2 + 1));
        
       
        String[] allOtherPages = links.split(",");
        for (String otherPage : allOtherPages) { 
            Text pageRankWithTotalLinks = new Text(pageRank + "\t" + allOtherPages.length);
            context.write(new Text(otherPage), pageRankWithTotalLinks); 
        }
        
        //We put the original links to produce the correct output
        context.write(new Text(page), new Text(PageRankParam.LINKS_SEPARATOR + links));
        
    }

}
