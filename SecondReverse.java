import java.io.IOException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;
import java.util.StringTokenizer;
import java.util.ArrayList; 
import java.util.Collections; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class SecondReverse extends Configured implements Tool {
	private static Configuration conf;
	private static Job job;
	public static class KeyMapper7 extends Mapper<Text,Text,Text,Text> {
		private Text OutputKey = new Text();
		   private Text OutputValue = new Text();

	  
		
		 public void map(Text key, Text value,Context context) throws IOException, InterruptedException
	     { //String s1="";
	         StringTokenizer itr2 = new StringTokenizer(key.toString());
	         // String[] k = itr2.nextToken().split(",");
	          String []k1 = itr2.nextToken().split("_");
	          
	          
	          
	          Scanner inp3file = new Scanner(new File("/home/akshay/Desktop/input5"));
	         // s1  = reviewdetect(k[0]);

	      	String o="";
	      	while (inp3file.hasNextLine()) {
	      		//String inp1[]=inpfile.nextLine().toString().split(" ");
	      		
	      		String inp[]=inp3file.nextLine().toString().split("_");
	          
	      		
	      		if(k1[0].toString().trim().equals(inp[0].toString().trim())){
	      		//o=inp[1].toString();
	      			OutputKey.set(k1[0]);
	      			
	      			OutputValue.set(value);
	      			context.write(OutputKey,OutputValue);
	             
	 		}
	     	}
	     	
	     	
	     	inp3file.close();
	         
	         
	     }
	} 
	public int run (String[] args) throws Exception {
		
		conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
	    job = Job.getInstance(conf,"Task 5");   
	   
	  
	        
	    
	    Configuration KeyMapper2Config = new Configuration(false);
	    ChainMapper.addMapper(job, KeyMapper7.class, Text.class, Text.class, Text.class, Text.class, KeyMapper2Config);

	   
	    job.setJarByClass(SecondReverse.class);
	    job.setNumReduceTasks(0);

	   // job.setReducerClass(KeyReducer4.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setInputFormatClass(KeyValueTextInputFormat.class);
		
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    return job.waitForCompletion(true) ? 0 : 1;
	    }

	public static void main(String[] args) {
		try {
			int res = ToolRunner.run(conf, new SecondReverse(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
