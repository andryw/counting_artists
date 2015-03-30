package com.example;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by andryw on 28/03/15.
 */
public class ArtistCount {
    public static class ArtistCountMapper extends  Mapper<LongWritable, Text, Text, IntWritable> {

        private Text word = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {

            String line = value.toString();
            String[] values = line.split(";");
            word.set(values[0]);
            context.write(word, one);
        }


    }


    public static class ArtistCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            //Sum all occurrences (list of values) of the word (key)
            for (IntWritable val : values) {
                sum += val.get();
            }

            //Emit the word (key) and the total of occurrences (value)
            context.write(key, new IntWritable(sum));
        }
    }


    public static void main(String[] args) throws Exception {
        //Set input and output path by the args, or use default paths
        String inputPath = "/home/andryw/Documents/nova/nova_coleta/*";
        String outputPath = "data/artistcount1/";

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ";");

        Job job = new Job(conf, "wordcount");

        //Set Output values.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //Set Mapper and Reducer Classes
        job.setMapperClass(ArtistCountMapper.class);
        job.setReducerClass(ArtistCountReduce.class);

        //Set the input and output. It reads a text file and saves on a text file.
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //Set the input and output path
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        //Delete the output path before run, to avoid exception
        FileSystem fs1 = FileSystem.get(conf);
        Path out1 = new Path(outputPath);
        fs1.delete(out1, true);

        //Run!
        job.waitForCompletion(true);
        System.out.println("=================================================");
        System.out.println("To see the results open the folder " + outputPath);
        System.out.println("=================================================");

    }



}