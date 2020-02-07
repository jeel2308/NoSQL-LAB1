package error404;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class error404 
{
	public static class errMapper extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException 
		{
			String str = value.toString();
			
			int i=0;
			while(i<str.length() && str.charAt(i)!='[')
			{
				i++;
			}
			i++;
			
			String time_stamp="";
			while(i<str.length() && str.charAt(i)!=':')
			{
				time_stamp+=str.charAt(i);
				i++;
			}
			i++;
			
			time_stamp+=" ";
			while(i<str.length() && str.charAt(i)!=']')
			{
				time_stamp+=str.charAt(i);
				i++;
			}
			
			while(i<str.length() && str.charAt(i)!='/')
			{
				i++;
			}
			
			time_stamp+=" ";
			while(i<str.length() && str.charAt(i)!=' ')
			{
				time_stamp+=str.charAt(i);
				i++;
			}
			
			while(i<str.length() && str.charAt(i)!='"')
			{
				i++;
			}
			i+=2;
			
			if(str.charAt(i)=='4' && str.charAt(i+1)=='0' && str.charAt(i+2)=='4')
			{
				value.set(time_stamp);
				context.write(value, new IntWritable(1));
			}
		}
	}
	
	public static class errReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
	{
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException 
		{
			
		}
	}
	
	public static void main(String[] args) throws Exception 
	{
		Configuration conf= new Configuration();
		Job job = Job.getInstance(conf,"error404");
		job.setJarByClass(error404.class);
		job.setNumReduceTasks(0);
		
		job.setMapperClass(errMapper.class);
		job.setReducerClass(errReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path outputPath = new Path(args[1]);
		//Configuring the input/output path from the filesystem into the job
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//deleting the output path automatically from hdfs so that we don't have to delete it explicitly
		outputPath.getFileSystem(conf).delete(outputPath,true);
		//exiting the job only if the flag value becomes false
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
