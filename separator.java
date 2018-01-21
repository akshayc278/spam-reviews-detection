import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.chain.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class separator extends Configured implements Tool{
	private static Configuration conf;
	private static Job job;
public static class keyMapper0 extends Mapper<Text,Text,Text,Text>{
	
		private Text OutputKey = new Text();
	    private Text OutputValue = new Text();
	    long i=0;
	    public void map(Text key, Text values, Context context) throws IOException, InterruptedException {
	    	
	    	
	    	
	    	
	    	 i++;
	    	 OutputKey.set(Long.toString(i)+"$");
      	     OutputValue.set(values);
			 context.write(OutputKey, OutputValue);
	    }

}
public int run (String[] args) throws Exception {
	
	conf = new Configuration();
	String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
    job = Job.getInstance(conf,"Task 0");   
   
    //*** FIRST MAPPER CLASS ***//
    Configuration KeyMapper1Config = new Configuration(false);
    ChainMapper.addMapper(job, keyMapper0.class, Text.class, Text.class, Text.class, Text.class, KeyMapper1Config);
        
    

   
    job.setJarByClass(separator.class);
    job.setNumReduceTasks(0);

   
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(KeyValueTextInputFormat.class);
	
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    return job.waitForCompletion(true) ? 0 : 1;
    }

public static void main(String[] args) {
	try {
		int res = ToolRunner.run(conf, new separator(), args);
		System.exit(res);
	} catch (Exception e) {
		e.printStackTrace();
	}
}
}
