package avgmarks;

import java.io.IOException;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q7_4 {

   
    public static class Map
            extends Mapper<LongWritable, Text, NullWritable, Text> {

        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String d[] = value.toString().split(",");

            if (d[1].contains("Gold")) {
                context.write(NullWritable.get(), new Text(d[1]));
            }
        }
    }

    
    public static class Reduce
            extends Reducer<NullWritable, Text, NullWritable, Text> {

        protected void reduce(NullWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text v : values) {
                context.write(NullWritable.get(), v);
            }
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movies with Gold in Title");

        job.setJarByClass(Q7_4.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // delete output folder if exists
        Path outputPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
