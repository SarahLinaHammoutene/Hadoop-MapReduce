package job2;

import job1.PageRankJob1Driver;
import job1.PageRankJob1Mapper;
import job1.PageRankJob1Reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class PageRankJob2Driver extends Configured implements Tool  {
	
 
  
    private static final String INPUT_PATH = "job1output";
 
    public int run(String[] args) throws Exception {
 
        Configuration conf = getConf();
        Job job = new Job(conf, "Job2");
 
        job.setJarByClass(PageRankJob2Driver.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PageRankJob2Mapper.class);
        
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PageRankJob2Reducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        //The name of the outputs files is job2output + the iteration number
        String inputFile = "job2output" + args[0];			
 
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(inputFile));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
    

}
