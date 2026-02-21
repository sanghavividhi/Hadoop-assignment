package avgmarks;

import java.io.IOException;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q7_5 {

   
    public static class Map
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String d[] = value.toString().split(",");

            if (d[2].contains("Drama") && d[2].contains("Romance")) {
                context.write(new Text("Drama_Romantic"), new IntWritable(1));
            }
        }
    }

   
    public static class Reduce
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int count = 0;
            for (IntWritable v : values) {
                count += v.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Drama and Romantic Movies Count");

        job.setJarByClass(Q7_5.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // delete output folder if exists
        Path outputPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
