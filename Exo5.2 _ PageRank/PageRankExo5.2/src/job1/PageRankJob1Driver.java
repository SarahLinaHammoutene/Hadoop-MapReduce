package job1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class PageRankJob1Driver extends Configured implements Tool {
	
    private static final String OUTPUT_PATH = "job1output";
 
    private static final String INPUT_PATH = "soc-Epinions1.txt";
 
    public int run(String[] args) throws Exception {
 
        Configuration conf = getConf();
        Job job = new Job(conf, "Job1");
 
        job.setJarByClass(PageRankJob1Driver.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PageRankJob1Mapper.class);
        
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PageRankJob1Reducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
   
}
