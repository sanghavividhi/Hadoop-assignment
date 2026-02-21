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

public class FemaleVotersCount {

    public static class VoterMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

    	protected void map(LongWritable key, Text value, Context context)
    	        throws IOException, InterruptedException {

    	    String data[] = value.toString().split(",");

    	    if (data[2].equalsIgnoreCase("Female")) {
    	        context.write(new Text("Female"), new IntWritable(1));
    	    }
    	}

    }

    // ===== Reducer =====
    public static class VoterReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        protected void reduce(Text key, Iterable<IntWritable> values,
                              Context context)
                throws IOException, InterruptedException {

            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }

            context.write(
                new Text("No. of female voters are :"),
                new IntWritable(count)
            );
        }
    }

    // ===== Main =====
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Female Voters Count");

        job.setJarByClass(FemaleVotersCount.class);
        job.setMapperClass(VoterMapper.class);
        job.setReducerClass(VoterReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path outputPath=new Path(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,outputPath);
		
		outputPath.getFileSystem(conf).delete(outputPath,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

