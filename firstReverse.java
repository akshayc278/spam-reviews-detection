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


public class firstReverse extends Configured implements Tool {
	private static Configuration conf;
	private static Job job;
public static class KeyMapper6 extends Mapper<Text,Text,Text,Text> {
	private Text OutputKey = new Text();
	   private Text OutputValue = new Text();

	
	 public void map(Text key, Text value,Context context) throws IOException, InterruptedException
     {
        
         StringTokenizer itr = new StringTokenizer(value.toString());
        
        	 String[] a = itr.nextToken().split(",");
        	 double Q1=Double.valueOf(a[0]);
          	 double Q3=Double.valueOf(a[1]);
          	double IQR = Q3-Q1;
         	double c1=Q3-IQR*1.5;
         	double c2=Q3+IQR*1.5;
 
         	String o="";
         Scanner in2file = new Scanner(new File("/home/akshay/Desktop/input3"));
         while (in2file.hasNextLine()) {
     		String in2[]=in2file.nextLine().toString().split(" ");
     		double p=Double.valueOf(in2[1]);
     		if(p<=c2 && p>=c1){
     			o= in2[0];
     		}else{
     			 o = in2[0];
     			 
     	     	
     			OutputKey.set(in2[0]);
                OutputValue.set(in2[0]);
                context.write(OutputKey,OutputValue);
     		}
     		}
         in2file.close();
         
     }
}
/*public static class KeyReducer8 extends Reducer<Text,Text,Text,Text> {
	private Text OutputKey = new Text();
	   private Text OutputValue = new Text();

  
	
	   public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
       {  //String s1="";
         StringTokenizer itr2 = new StringTokenizer(key.toString());
        // String[] k = itr2.nextToken().split(",");
         String []k1 = itr2.nextToken().split("_");
        
         Scanner inp3file = new Scanner(new File("/home/akshay/Desktop/input1"));
        // s1  = reviewdetect(k[0]);

     	String o="";
     	while (inp3file.hasNextLine()) {
     		//String inp1[]=inpfile.nextLine().toString().split(" ");
     		
     		String inp[]=inp3file.nextLine().toString().split("_");
     		
     		
     		if(k1[0].toString().trim().equals(inp[0].toString().trim())){
     		      o=inp[1].toString();
     			OutputKey.set(k1[0]);
         		
         		
         		OutputValue.set(o);
     		
            
            context.write(OutputKey,OutputValue);
 		}
     	}
     	
     	
     	inp3file.close();
         
         
     }
} */
public int run (String[] args) throws Exception {
	
	conf = new Configuration();
	String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
    job = Job.getInstance(conf,"Task 4");   
   
    //*** FIRST MAPPER CLASS ***//
    Configuration KeyMapper1Config = new Configuration(false);
    ChainMapper.addMapper(job, KeyMapper6.class, Text.class, Text.class, Text.class, Text.class, KeyMapper1Config);
        
    //*** SECOND MAPPER CLASS ***//
   //Configuration KeyMapper2Config = new Configuration(false);
    //ChainMapper.addMapper(job, KeyMapper7.class, Text.class, Text.class, Text.class, Text.class, KeyMapper2Config);

   
    job.setJarByClass(firstReverse.class);
   job.setNumReduceTasks(0);

    //  job.setReducerClass(KeyReducer8.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(KeyValueTextInputFormat.class);
	
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    return job.waitForCompletion(true) ? 0 : 1;
    }

public static void main(String[] args) {
	try {
		int res = ToolRunner.run(conf, new firstReverse(), args);
		System.exit(res);
	} catch (Exception e) {
		e.printStackTrace();
	}
}
}