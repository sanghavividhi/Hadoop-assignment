package com.assignment.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MultiAssignment {

    // ==============================
    // 1. WORD COUNT
    // ==============================

    public static class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (String w : words)
                context.write(new Text(w), one);
        }
    }

    public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            context.write(key, new IntWritable(sum));
        }
    }

    // ==============================
    // 2. USER ACTIVITY JOIN
    // ==============================

    public static class LoginMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] p = value.toString().split(",");
            context.write(new Text(p[0]), new Text("LOGIN:" + p[1]));
        }
    }

    public static class ActivityMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] p = value.toString().split(",");
            context.write(new Text(p[0]), new Text("ACTIVITY:" + p[1]));
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String login = "", activity = "";
            for (Text v : values) {
                if (v.toString().startsWith("LOGIN"))
                    login = v.toString();
                else
                    activity = v.toString();
            }
            context.write(key, new Text(login + " " + activity));
        }
    }

    // ==============================
    // 3. SALES AGGREGATION
    // ==============================

    public static class SalesMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] p = value.toString().split(",");
            context.write(new Text(p[0]), new Text(p[1] + "," + p[2]));
        }
    }

    public static class SalesReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int qty = 0, revenue = 0;
            for (Text v : values) {
                String[] p = v.toString().split(",");
                qty += Integer.parseInt(p[0]);
                revenue += Integer.parseInt(p[1]);
            }
            context.write(key, new Text(qty + "," + revenue));
        }
    }

    // ==============================
    // 4. TEMPERATURE ANALYSIS
    // ==============================

    public static class TempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] p = value.toString().split(",");
            context.write(new Text(p[0]), new IntWritable(Integer.parseInt(p[1])));
        }
    }

    public static class TempReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0, count = 0;
            for (IntWritable v : values) {
                sum += v.get();
                count++;
            }
            context.write(key, new DoubleWritable((double) sum / count));
        }
    }

    // ==============================
    // 5. STOCK WEIGHTED AVG
    // ==============================

    public static class StockMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] p = value.toString().split(",");
            context.write(new Text(p[0]), new Text(p[1]));
        }
    }

    public static class StockReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double price = 0, volume = 0;
            for (Text v : values) {
                double val = Double.parseDouble(v.toString());
                if (price == 0)
                    price = val;
                else
                    volume = val;
            }
            context.write(key, new DoubleWritable(price));
        }
    }

    // ==============================
    // 6. LOG FILE PROCESSING
    // ==============================

    public static class LogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(value, one);
        }
    }

    // ==============================
    // 7. CUSTOMER PURCHASE
    // ==============================

    public static class CustomerMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] p = value.toString().split(",");
            context.write(new Text(p[0]), new Text("CUST:" + p[1]));
        }
    }

    public static class PurchaseMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] p = value.toString().split(",");
            context.write(new Text(p[0]), new Text("PUR:" + p[1]));
        }
    }

    // ==============================
    // 8. MOVIE RECOMMENDATION
    // ==============================

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] p = value.toString().split(",");
            context.write(new Text(p[0]), new Text("RATING:" + p[1]));
        }
    }

    public static class GenreMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] p = value.toString().split(",");
            context.write(new Text(p[0]), new Text("GENRE:" + p[1]));
        }
    }

    // ==============================
    // 9. CLICKSTREAM ANALYSIS
    // ==============================

    public static class ClickMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] p = value.toString().split(",");
            context.write(new Text(p[0]), new Text("CLICK:" + p[1]));
        }
    }

    // ==============================
    // 10. SENTIMENT ANALYSIS
    // ==============================

    public static class SentimentMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] p = value.toString().split(",");
            context.write(new Text(p[0]), new Text("SENT:" + p[1]));
        }
    }

    // ==============================
    // MAIN METHOD
    // ==============================

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "All 10 Assignment");
        job.setJarByClass(MultiAssignment.class);

        // For simplicity, you can configure specific job logic
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, WCMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}