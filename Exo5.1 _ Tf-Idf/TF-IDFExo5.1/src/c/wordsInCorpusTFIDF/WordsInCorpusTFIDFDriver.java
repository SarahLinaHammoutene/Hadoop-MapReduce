package c.wordsInCorpusTFIDF;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import b.wordCountsForDocs.WordCountsInDocumentsDriver;
import a.wordFrequenceInDoc.WordFrequenceInDocDriver;
 


public class WordsInCorpusTFIDFDriver extends Configured implements Tool {
 
    private static final String OUTPUT_PATH = "c-tf-idf";
 
    private static final String INPUT_PATH = "b-word-counts";
 
    public int run(String[] args) throws Exception {
 
        Configuration conf = getConf();
        Job job = new Job(conf, "Word in Corpus, TF-IDF");
 
        job.setJarByClass(WordsInCorpusTFIDFDriver.class);
        job.setMapperClass(WordsInCorpusTFIDFMapper.class);
        job.setReducerClass(WordsInCorpusTFIDFReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
 
        //Getting the number of documents from the  input directory.
        Path inputPath = new Path("input");
        FileSystem fs = inputPath.getFileSystem(conf);
        FileStatus[] stat = fs.listStatus(inputPath);
 
      
        job.setJobName(String.valueOf(stat.length));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
 
   
}