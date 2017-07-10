import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class q7 
{
public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>
{
	public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException
	{
		String[]line = value.toString().split("\t");
		String year = line[7];
	con.write(new Text("key"), new Text (year));
	}
}
public static class MyReducer extends Reducer<Text,Text,NullWritable,Text>
{
	public void reduce(Text key,Iterable<Text> value,Context con) throws IOException, InterruptedException
	{
		int count1=0,count2=0,count3=0,count4=0,count5=0,count6=0,total=0;;
		for(Text val: value)
		{
			total++;
			if(val.toString().contains("2011"))
			{
				count1++;
			}
			else if(val.toString().contains("2012"))
			{
				count2++;
			}
			else if(val.toString().contains("2013"))
			{
				count3++;
			}
			else if(val.toString().contains("2014"))
			{
				count4++;
			}
			else if(val.toString().contains("2015"))
			{
				count5++;
			}
			else if(val.toString().contains("2016"))
			{
				count6++;
			} 
		}
		String out = "The number of petitions in 2011 is"+"\t"+count1+"\n"+"The number of petitions in 2012 is"+"\t"+count2+"\n"+"The number of petitions in 2013 is"+"\t"+count3+"\n"+"The number of petitions in 2014 is"+"\t"+count4+"\n"+"The number of petitions in 2015 is"+"\t"+count5+"\n"+"The number of petitions in 2016 is"+"\t"+count6+"\n"+"The total number of petitions bw 2011 and 2016 is"+"\t"+total;
		con.write(null, new Text(out));
	}
}
public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException
{
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "No of petitions");
	job.setJarByClass(q7.class);
	job.setMapperClass(MyMapper.class);
	job.setReducerClass(MyReducer.class);
	job.setNumReduceTasks(1);

	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
   FileOutputFormat.setOutputPath(job, new Path(args[1]));
  System.exit(job.waitForCompletion(true) ? 0 : 1);
}

}
