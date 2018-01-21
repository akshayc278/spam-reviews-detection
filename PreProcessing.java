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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.WhitespaceTokenizer;
import opennlp.maxent.*;

public class PreProcessing extends Configured implements Tool {
	
	private static Configuration conf;
	private static Job job;

	public static class KeyMapper1 extends Mapper<Text,Text,Text,Text> {
        
		private Text OutputKey = new Text();
		private Text OutputValue = new Text();
		
		public static String[] sentenceDetect (String input) throws IOException {
			InputStream modelIn = new FileInputStream("/home/akshay/Desktop/en-sent.bin");
			SentenceModel model = new SentenceModel(modelIn);
			if (modelIn != null)  
				modelIn.close(); 
			
			SentenceDetectorME sentenceDetector = new SentenceDetectorME(model);
	        String sentences[] = sentenceDetector.sentDetect(input);
	        return sentences;
		}

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {	
			String val = "";
			String sentence[];
			val = value.toString();
			sentence = sentenceDetect(val);
			for (String s : sentence) {
				OutputKey.set(key+"_");
				OutputValue.set(s);
				context.write(OutputKey, OutputValue);
			}
		}
	}
	
public static class KeyMapper2 extends Mapper<Text,Text,Text,Text> {
        
		private Text result = new Text();

        public static String sentenceShort (String i) throws IOException,FileNotFoundException {
		    
			ArrayList<String> phrasein = new ArrayList<String>();
			ArrayList<String> phraseout = new ArrayList<String>();
			ArrayList<String> wordsList = new ArrayList<String>();
		    Set<String> stopWordsSet = new HashSet<String>();
            Scanner phraseinfile,phraseoutfile,stopwordsfile;
			String alpha = i.trim().replaceAll("[^a-zA-Z0-9\\s]", "").replaceAll("\\s+", " ");
			phraseinfile = new Scanner(new File("/home/akshay/Desktop/phrasein.txt"));
			phraseoutfile = new Scanner(new File("/home/akshay/Desktop/phraseout.txt"));
			stopwordsfile = new Scanner(new File("/home/akshay/Desktop/Stopword.txt"));
    	    
			String store = "";
        	while (phraseinfile.hasNextLine()) {
        	  phrasein.add(phraseinfile.nextLine());
        	}
        	phraseinfile.close();
        	
        	while (phraseoutfile.hasNextLine()) {
        	  phraseout.add(phraseoutfile.nextLine());
        	}
        	phraseoutfile.close();
        	
        	while (stopwordsfile.hasNextLine()) {
    	    	stopWordsSet.add(stopwordsfile.nextLine());
    	    }
    	    stopwordsfile.close();
    	    
        	String[] arrin = phrasein.toArray(new String[0]);
        	String[] arrout = phraseout.toArray(new String[0]);
        	
        	String beta = null;
        	for(int k=0;k<arrin.length;k++){
        		beta = alpha.replaceAll(arrin[k],arrout[k]);
        	    alpha = beta;
        	}
        	
			String[] words = beta.split(" ");
	    	
		    for(String word : words) {
		    String wordCompare = word.toLowerCase();
		    if(!stopWordsSet.contains(wordCompare))
		    wordsList.add(word);
		    }
		    
		    for (String str : wordsList) {
		    	store += str+" ";
		    }	
		    String t = store.trim().toLowerCase();
		    store = " ";
		    wordsList.clear();
		    return t;
        }
        
        public void map(Text key, Text value,Context context) throws IOException, InterruptedException
        {
            String s="";
            s /*+= ","+*/ = sentenceShort(value.toString());
            //s = s.replaceFirst(",","");
            result.set(s);
            context.write(key, result);
        }
    }
public static class KeyReducer1 extends Reducer <Text,Text,Text,Text> {
	 
	private Text result = new Text();
	  
	public static String posTag(String i) {
	   String op = "";
	   InputStream modelIn = null;
	   POSModel model = null;
	   try {
	    modelIn = new FileInputStream("/home/akshay/Desktop/en-pos-maxent.bin");
	    model = new POSModel(modelIn);
	   } catch (IOException e) {
	    e.printStackTrace();
	   } finally {
	    if (modelIn != null) {
	     try {
	      modelIn.close();
	     } catch (IOException e) {
	    	 e.printStackTrace();
	     }
	    }
	   }
	   POSTaggerME tagger = new POSTaggerME(model);
	   String[] sent = WhitespaceTokenizer.INSTANCE.tokenize(i);
	   String[] tags = tagger.tag(sent);
	   for (int j = 0; j < sent.length; j++) {
	    op += tags[j] + "," + sent[j] + " ";
	   }
	   return op;
	  }

	public void reduce(Text key, Iterable <Text> values, Context context) throws IOException,InterruptedException {
	   
	   String value, sentence, set = "";

	   for (Text val: values) {
	    value = val.toString();
	    sentence = posTag(value);
	    set += sentence;
	   }
	   result.set(set);
	   context.write(key,result);
	  }
}

public int run (String[] args) throws Exception {
	
	conf = new Configuration();
	String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
    job = Job.getInstance(conf,"Task 1");   
   
    //*** FIRST MAPPER CLASS ***//
    Configuration KeyMapper1Config = new Configuration(false);
    ChainMapper.addMapper(job, KeyMapper1.class, Text.class, Text.class, Text.class, Text.class, KeyMapper1Config);
        
    //*** SECOND MAPPER CLASS ***//
    Configuration KeyMapper2Config = new Configuration(false);
    ChainMapper.addMapper(job, KeyMapper2.class, Text.class, Text.class, Text.class, Text.class, KeyMapper2Config);

   
    job.setJarByClass(PreProcessing.class);
    job.setReducerClass(KeyReducer1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(KeyValueTextInputFormat.class);
	
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    return job.waitForCompletion(true) ? 0 : 1;
    }

public static void main(String[] args) {
	try {
		int res = ToolRunner.run(conf, new PreProcessing(), args);
		System.exit(res);
	} catch (Exception e) {
		e.printStackTrace();
	}
}
}