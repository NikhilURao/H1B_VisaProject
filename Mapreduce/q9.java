import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class q9 
{
public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>
{
	public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException
	{
		String [] line = value.toString().split("\t");
		String emp_name = line[2];
		String case_status = line[1];
		con.write(new Text(line[2]) ,new Text(case_status));
	}
}

public static class MyReducer extends Reducer<Text,Text,Text,Text>
{
	public void reduce(Text key,Iterable<Text> value,Context con) throws IOException, InterruptedException
	{	float count=0,count1=0;
		float Per=0;
		for(Text t:value)
		{
			count++;
			if (count>1000)
			{
				if(t.toString().contains("CERTIFIED"))
				{
				count1++;
				}
			}
		}
	Per =((count1*100)/count);
	if(Per>70.0)
	{
	con.write(new Text(key), new Text(count1+" "+count+" "+Per));
	}
	}
//Sorting continued in Pig(Q9.pig)
}
public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException
{
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf , "Q9");
	job.setJarByClass(q6.class);
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
