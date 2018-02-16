package c.wordsInCorpusTFIDF;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class WordsInCorpusTFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {
 
    public WordsInCorpusTFIDFMapper() {
    }
 
   
     //     Output: <word, documentName=NbOfOccurenceInTheDoc/TotalNumberOfWordsInTheDoc>
     
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] wordAndCounters = value.toString().split("\t");
        String[] wordAndDoc = wordAndCounters[0].split("@");                 
        context.write(new Text(wordAndDoc[0]), new Text(wordAndDoc[1] + "=" + wordAndCounters[1]));
    }
}