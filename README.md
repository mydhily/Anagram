# Anagram
package p1;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Anagram
{

public  static class AnagramMapper  extends Mapper<LongWritable, Text, Text, Text>{
	private Text sortedText = new Text();
    private Text orginalText = new Text();
    @Override
public void map(LongWritable key ,Text value,Context context) throws IOException, InterruptedException
{
	String word = value.toString();
	char[] wordChars = word.toCharArray();
	Arrays.sort(wordChars);
	String sortedWord = new String(wordChars);
	sortedText.set(sortedWord);
	orginalText.set(word);
	context.write(sortedText, orginalText);
	
}
}  
public  static class AnagramReducer  extends Reducer<Text, Text, Text, Text>{
	private Text outputKey = new Text();
    private Text outputValue = new Text();
    public void reduce(Text key, Iterator<Text> values, Context context)
    	      throws IOException, InterruptedException {
    	String output = "";
        while(values.hasNext())
        {
                Text anagam = values.next();
                output = output + anagam.toString() + "~";
        }
        StringTokenizer outputTokenizer = new StringTokenizer(output,"~");
        if(outputTokenizer.countTokens()>=2)
        {
                output = output.replace("~", ",");
                outputKey.set(key.toString());
                outputValue.set(output);
               context.write(outputKey, outputValue);
        }

}
}

public static void main(String[] args) throws Exception {
	
	Configuration conf = new Configuration();
	Job job = new Job(conf,"Anagram");
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setMapperClass(AnagramMapper.class);
	job.setReducerClass(AnagramReducer.class);
	job.setInputFormatClass(TextInputFormat.class);	
	job.setOutputKeyClass(TextOutputFormat.class);
		
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.waitForCompletion(true);
	
	
	
	
}
}
