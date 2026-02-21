package avgmarks;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class tamprature {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>
	{
		 protected void map(LongWritable key, Text value, Context context)
	                throws IOException, InterruptedException {

	            // No header, directly split
	            String[] data = value.toString().split(",");

	            String country = data[0];
	            String year = data[1];
	            String temp = data[2];

	            // key = country, value = year,temp
	            context.write(new Text(country), new Text(year + "," + temp));
	        }
	}
	public static class MinTempReducer
    extends Reducer<Text, Text, Text, Text> {

protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

    int minTemp = Integer.MAX_VALUE;
    String minYear = "";

    for (Text val : values) {
        String[] parts = val.toString().split(",");
        int year = Integer.parseInt(parts[0]);
        int temp = Integer.parseInt(parts[1]);

        if (temp < minTemp) {
            minTemp = temp;
            minYear = parts[0];
        }
    }

    context.write(key, new Text(minYear + "\t" + minTemp));
}
}
	public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Country Minimum Temperature");

        job.setJarByClass(tamprature.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(MinTempReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        Path outputPath=new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        outputPath.getFileSystem(conf).delete(outputPath,true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
