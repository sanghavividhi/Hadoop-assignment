package avgmarks;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class avrageofmarks {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
	{
	    public void map(LongWritable key, Text value, Context context)
	            throws IOException, InterruptedException
	    {
	        String data[] = value.toString().split(",");
	        int marks = Integer.parseInt(data[2]);
	        context.write(new Text(data[1]), new IntWritable(marks));
	    }
	}
	public static class Reduce extends Reducer<Text, IntWritable, Text, FloatWritable>
	{
	    public void reduce(Text key, Iterable<IntWritable> values, Context context)
	            throws IOException, InterruptedException
	    {
	        int sum = 0, cnt = 0;

	        for (IntWritable val : values)
	        {
	            sum += val.get();
	            cnt++;
	        }

	        float avg = (float) sum / cnt;   // ✔ correct average
	        context.write(key, new FloatWritable(avg));
	    }
	}
	public static void main(String args[])throws Exception
	{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"WordCount");
		
		job.setJarByClass(avrageofmarks.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	
		Path outputPath=new Path(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,outputPath);
		
		outputPath.getFileSystem(conf).delete(outputPath,true);
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
