package d.rankingTfIdf;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import a.wordFrequenceInDoc.WordFrequenceInDocDriver;
import b.wordCountsForDocs.WordCountsInDocumentsDriver;
import c.wordsInCorpusTFIDF.WordsInCorpusTFIDFDriver;


public class TfIdfRankingDriver extends Configured implements Tool  {

	
    private static final String OUTPUT_PATH = "d-ranking";
 
  
    private static final String INPUT_PATH = "c-tf-idf";
    
    public int run(String[] args) throws Exception {
    	 
        Configuration conf = getConf();
        Job job = new Job(conf, "Ranking Tf-Idf");
 
        job.setJarByClass(TfIdfRankingDriver.class);
        
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(TfIdfRankingMapper.class);
        
        
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setReducerClass(TfIdfRankingReducer.class);

        
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
   
    public static void main(String[] args) throws Exception {
        int res1 = ToolRunner.run(new Configuration(), new WordFrequenceInDocDriver(), args);
        int res2 = ToolRunner.run(new Configuration(), new WordCountsInDocumentsDriver(), args);
        int res3 = ToolRunner.run(new Configuration(), new WordsInCorpusTFIDFDriver(), args);
        int res4 = ToolRunner.run(new Configuration(), new TfIdfRankingDriver(), args);


        System.exit(res4);
    }
    
    
}
