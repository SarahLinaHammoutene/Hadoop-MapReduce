package cs.bigdata.Tutorial2;


import java.io.*;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;



public class ReadCompact {

	public static void main(String[] args) throws IOException {
		
		String inputFilePath = args[0];    
		String outputFilePath = args[1];

  		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in = new BufferedInputStream(new FileInputStream(inputFilePath));
		
		//Destination file in HDFS
		OutputStream out = fs.create(new Path(outputFilePath));
		PrintWriter writer = new PrintWriter(outputFilePath, "UTF-8");

		int nbLines=0;
		
		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);

			String line = br.readLine();

			// read line by line
			for (int i=0; i<22; i++){
				if (line !=null) line = br.readLine();			
			}
			
			
			while (line !=null){
				// Process of the current line
		
	            String USAF = line.substring(0,6);
	            String name = line.substring(13,42);
	            String country = line.substring(43,45);
	            String elevation = line.substring(74,81);

	            nbLines = nbLines+1;
	            String data = "USAF= " + USAF + " , name=" + name + " , country=" + country + " , elevation=" + elevation;
	            line = br.readLine();		
	            
	            //Write in a file
	            writer.println(data);
	        
			}
			System.out.println(nbLines);
            writer.close();

			
		}
		finally{
			//close the file
			in.close();
			fs.close();
		}
 
		
		
	}

}
