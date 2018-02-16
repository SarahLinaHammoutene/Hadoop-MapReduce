package a.wordFrequenceInDoc;


import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WordFrequenceInDocMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
 
    public WordFrequenceInDocMapper() {
    }
    
    
    //We define the stop words
	  List<String> stop_words = Arrays.asList("  ",",", "-", "_", "...", "i'm", "you're", "he's", "she's", "re",
			"in", "all","rt", "of", "i", "you","...", "i","me", "my", "myself", "we", "our", "ours",
			"ourselves", "you", "your", "yours","as", "not", "but", "an", "a", "the", "on", "at",
            "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers",
            "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves",
            "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are",
            "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does",
            "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until",
            "while", "of", "at", "by", "for", "with", "about", "against", "between", "into",
            "through", "during", "before", "after", "above", "below", "to", "from", "up", "down",
            "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here",
            "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more",
            "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so",
            "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now", "doesn", "don", "lol", "lmfao");
	   
	    
	   /*   
	      @param key is the key of the current line in the file;
	      @param value is the line from the file
     
	      Output <"word", "documentName@offset"> 
	      */
	     
	  
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        // Compile all the words using regex
	        Pattern p = Pattern.compile("\\w+");
	        Matcher m = p.matcher(value.toString());
	 
	        // Get the name of the file from the inputsplit in the context
	        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	       
	        // build the values and write <k,v> pairs through the context
	       
	        StringBuilder valueBuilder = new StringBuilder();
	        String line = value.toString().toLowerCase();
				
			//Remove all the URLs (*://*)
			String cleartext = line.replaceAll("[\\S]+://[\\S]+", " ");
				  
			//Remove the links to websites (www.*)
			cleartext= cleartext.replaceAll("www.[\\S]+", " ");	
							
			//Remove the tags knowning that they start with @
			cleartext= cleartext.replaceAll("@[\\S]+", " ");		
				   
			//Remove the non alpha numeric characters and convert the string to lowercase 
			cleartext = cleartext.replaceAll("\\P{Alnum}", " ").toLowerCase();	
				   
			//We split the words contained in the string when we find a space, point or : 
			String[] result = cleartext.split("\\,|\\s|\\.|:| ");	  
				   
			
			//At the same time we save the cleaned tweets for weka  
			String s= "";		   
				   
			for (int x=0; x<result.length; x++)	   
			if ( (! stop_words.contains(result[x]) && (result[x].length()>2)))   		
			//Check if the word is not a stop word and its length is >2
				{
					valueBuilder = new StringBuilder();
					valueBuilder.append(result[x]);
					valueBuilder.append("@");
					valueBuilder.append(fileName);

					context.write(new Text(valueBuilder.toString()), new IntWritable(1));
				}
			
				
	    }
	   
    
    

}
