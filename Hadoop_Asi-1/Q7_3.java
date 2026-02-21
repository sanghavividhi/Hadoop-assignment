package avgmarks;
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

public class Q7_3 {


    public static class Map
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String d[] = value.toString().split(",");

            
            if (d[2].isEmpty()) {
                context.write(new Text("Missing_Genres"), new IntWritable(1));
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
        Job job = Job.getInstance(conf, "Missing Genres Count");

        job.setJarByClass(Q7_3.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path outputPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);	
		outputPath.getFileSystem(conf).delete(outputPath,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
