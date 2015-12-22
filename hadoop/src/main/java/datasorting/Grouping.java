package datasorting;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Grouping {

	public static class groupmapper extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		
		Text text=new Text();
		
		IntWritable numvalue=new IntWritable();
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
			String Line=value.toString();
			String [] wholeline=Line.split(",");
			text.set(wholeline[0]+","+wholeline[1]);
			numvalue.set(Integer.parseInt(wholeline[2]));
			context.write(text,numvalue);			
			
		}
	}
	
	public static class groupreduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		//String first;
		IntWritable result=new IntWritable();
		int sum = 0;
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
		{
		for(IntWritable val: values)
		{
			
			sum+=val.get();
		}
		result.set(sum);
		context.write(key,result);
		
			
		}
		
	
		
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf," Grouping");
        job.setJarByClass(Grouping.class);
        job.setMapperClass(groupmapper.class);
        job.setReducerClass(groupreduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args [1]));
        System.exit(job.waitForCompletion(true)?0:1);
	
	}

}
