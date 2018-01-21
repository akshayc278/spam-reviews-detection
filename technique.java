import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
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




public class technique extends Configured implements Tool{
	public static class keyMapper5 extends Mapper<Text,Text,Text,Text>{
		
		private Text OutputKey = new Text();
	    private Text OutputValue = new Text();
	    public void map(Text key, Text values, Context context) throws IOException, InterruptedException {
	    	//String a[] = key.toString().split(" ");
	    	Double i = Double.valueOf(values.toString());
	    	 OutputKey.set("key");
      	     OutputValue.set(" "+i.toString());
			 context.write(OutputKey, OutputValue);
	    }
	}
	 public static class KeyReducer3 extends Reducer<Text,Text,Text,Text> {
		 ArrayList<Double> tList = new ArrayList<Double>();
	    	private Text result = new Text();
	    	private Text tkey = new Text();
	    	 public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
		        {
		        	//String s[] = key.toString().split(" ");
		        	
		        	double Q1 = 0.0;
		        	double Q3 = 0.0;
		            for (Text val : values)
		            {
		            Double dval = Double.valueOf(val.toString());
		            	tList.add(dval);
		           
		            }
		            Collections.sort(tList);
		            int size  = tList.size();
		            size=size/2;
		            if(size%2 == 0){ 
		           		int firsthalf = size/2; 
		             			Q1  = tList.get(firsthalf); 
		             			Q3 = tList.get(3*firsthalf);
		             		}else { 
		             			int firsthalf = (size + 1)/2; 
		           			Q1 = tList.get(firsthalf -1); 
		           			Q3 = tList.get(3*(firsthalf -1)); 
		           		} 

		            
		            tkey.set(key);
		            result.set(String.valueOf(Q1)+","+String.valueOf(Q3));
		            context.write(tkey,result);
		    }
	 }
	 
	 static Configuration conf;
	    
	    public int run (String[] args) throws Exception {
			
			conf = new Configuration();
			String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
		    Job job = Job.getInstance(conf,"Task 3");   
		   
		    //*** FIRST MAPPER CLASS ***//
		    Configuration KeyMapper5Config = new Configuration(false);
		    ChainMapper.addMapper(job, keyMapper5.class, Text.class, Text.class, Text.class, Text.class, KeyMapper5Config);
		        
		   
		        
		    job.setJarByClass(technique.class);
		    

		    job.setReducerClass(KeyReducer3.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    job.setInputFormatClass(KeyValueTextInputFormat.class);
			
		    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	        return job.waitForCompletion(true) ? 0 : 1;
		    }
	    public static void main(String[] args) {
	    	try {
				int res = ToolRunner.run(conf, new technique(), args);
				System.exit(res);
			} catch (Exception e) {
				e.printStackTrace();
			}
	    }
}
