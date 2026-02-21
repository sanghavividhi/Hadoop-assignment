package avgmarks;

import java.io.IOException;

import javax.naming.Context;

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

public class Q7_2 {

    
    public static class Map
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String d[] = value.toString().split(",");

            if (d[2].contains("Documentary") && d[3].equals("1995")) {
                context.write(new Text("Documentary_1995"), new IntWritable(1));
            }
        }
    }

   
    public static class Reduce
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) {
                sum = sum + v.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

  
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Documentary Movies 1995");

        job.setJarByClass(Q7_2.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path outputPath=new Path(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,outputPath);
		
		outputPath.getFileSystem(conf).delete(outputPath,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
