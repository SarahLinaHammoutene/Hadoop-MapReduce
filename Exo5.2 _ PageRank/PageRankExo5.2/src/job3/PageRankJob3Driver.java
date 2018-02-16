package job3;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Set;

import job1.PageRankJob1Driver;
import job2.PageRankJob2Driver;
import job2.PageRankJob2Mapper;
import job2.PageRankJob2Reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import param.PageRankParam;


public class PageRankJob3Driver extends Configured implements Tool  {

	
    private static final String OUTPUT_PATH = "job3output";
 
    // We take the last output file of the previous job
    private static final String INPUT_PATH = "job2output" + (PageRankParam.nbIterations-1) ;
    
    public int run(String[] args) throws Exception {
    	 
        Configuration conf = getConf();
        Job job = new Job(conf, "Job3");
 
        job.setJarByClass(PageRankJob3Driver.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PageRankJob3Mapper.class);
        
        
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PageRankJob3Reducer.class);

        
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    public static void main(String[] args) throws Exception {
        int res1 = ToolRunner.run(new Configuration(), new PageRankJob1Driver(), args);
        
        //Run job 2 several times corresponding to the number of iterations specified 
        for (int i=0; i< PageRankParam.nbIterations; i++)
        	{   String [] arguments = new String[1];
        		arguments[0] = Integer.toString(i);
        		int res2 = ToolRunner.run(new Configuration(), new PageRankJob2Driver(),arguments); }
        
        int res3 = ToolRunner.run(new Configuration(), new PageRankJob3Driver(), args);
        System.exit(res3);
    }
    
    
}
