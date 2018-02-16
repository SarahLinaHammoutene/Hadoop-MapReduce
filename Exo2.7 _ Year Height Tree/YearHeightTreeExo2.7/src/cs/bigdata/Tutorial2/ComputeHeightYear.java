package cs.bigdata.Tutorial2;


import java.io.*;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;



public class ComputeHeightYear {

	public static void main(String[] args) throws IOException {
		

		String localSrc = "/home/cloudera/Downloads/arbres.csv";
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			
			line = br.readLine();
			
			while (line !=null){
				// Process of the current line
				//ArrayList<Tree> results = new ArrayList <Tree> (); 
				
				String [] result = line.split(";");

				//Create the tree 
				TreeParis arbre= new TreeParis (result);
				arbre.printTree(arbre);				
				line = br.readLine();
			}
			
		}
		finally{
			//close the file
			in.close();
			fs.close();
		}
 
		
		
	}

}
