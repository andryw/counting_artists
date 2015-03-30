package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * Created by andryw on 28/03/15.
 */

//class StringComp implements WritableComparable{
//
//    private String valor;
//    @Override
//    public int compareTo(Object o) {
//        return 0;
//    }
//
//    @Override
//    public void write(DataOutput dataOutput) throws IOException {
//        dataOutput.writeChars(valor);
//    }
//
//    @Override
//    public void readFields(DataInput dataInput) throws IOException {
//        dataInput.readUTF();
//    }
//}
public class ArtistCoocurrence {

    public static class AggregateArtistsByFileMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

            String line = value.toString();
            String[] parts = line.split(";");
            if (Integer.valueOf(parts[1]) > 10)
                context.write(new Text(fileName), new Text(parts[0]));
        }


    }



    public static class ArtistsPairsReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Iterator<Text> it = values.iterator();
            List<Text> list = new ArrayList();
            while (it.hasNext()) {
                String arts = it.next().toString();
                list.add(new Text(arts));
            }
            for(int i = 0; i < list.size(); i++){
                Text artist1 = list.get(i);

                for(int j = i+1; j < list.size(); j++){
                    Text artist2 = list.get(j);
                    if (!artist1.equals(artist2)){
                        if (artist1.compareTo(artist2)< 0){
                            context.write(artist1, artist2);
                        }else{
                            context.write(artist2, artist1);

                        }

                    }
                }
            }
        }
    }


    public static class ArtistsPairsMapper extends Mapper<Text, Text, Text, Text> {

        public void map(Text artist1, Text artist2, Context context)  throws IOException, InterruptedException {

            context.write(artist1, artist2);
        }


    }
    public static class ArtistCoocurrenceReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            HashMap<String, Integer> map = new HashMap<String, Integer>();

            for (Text text : values) {
                Text newText = new Text(text);
                Integer currentValue = map.get(newText.toString());
                if (currentValue == null) {
                    map.put(newText.toString(), 1);
                }else{
                    map.put(newText.toString(), currentValue+ 1);

                }
            }

            for (String keya : map.keySet() ){
                if (map.get(keya) > 10)
                    context.write(key, new Text (keya + ";" + map.get(keya)));

            }
        }
    }



    public static void main(String[] args) throws Exception {
        //Set input and output path by the args, or use default paths
        long start = System.currentTimeMillis();

//        String inputPath = "/local/data/n10000/*.txt";
//        String outputPath = "/local/artists_output/out/";
//        String temPath = "/local/artists_temp/temp/";

        String inputPath = args[0];
        String temPath = args[1];
        String outputPath = args[2];

        GenerateUserVectors(SequenceFileOutputFormat.class,inputPath,temPath);

        ComputeCoOcurrence(TextOutputFormat.class,temPath,outputPath);
        long elapsedTimeMillis = System.currentTimeMillis()-start;

        System.out.println("=================================================");
        System.out.println("TIME>>> " + elapsedTimeMillis/60000F);
        System.out.println("=================================================");

    }


    private static void GenerateUserVectors(Class outputFormatClass,String inputPath,String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        runJob("To User Vector",AggregateArtistsByFileMapper.class,ArtistsPairsReducer.class,
                Text.class, Text.class, Text.class, Text.class,
                TextInputFormat.class,outputFormatClass,
                inputPath,outputPath);
    }

    private static void ComputeCoOcurrence(Class outputFormatClass,String inputPath,String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        runJob("To Coocurrence Matrix",ArtistsPairsMapper.class,ArtistCoocurrenceReducer.class,
                Text.class, Text.class, Text.class, HashMap.class,
                SequenceFileInputFormat.class,outputFormatClass,
                inputPath,outputPath);
    }



    private static void runJob(String jobName,Class mapperClass, Class reducerClass, Class mapOutputKeyClass,
                               Class mapOutputValueClass, Class outputKeyClass, Class outputValueClass,
                               Class inputFormatClass, Class outputFormatClass, String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ";");
//        conf.set("mapred.compress.map.output", "true");
//        conf.setClass("mapred.map.output.compression.codec", GzipCodec.class,CompressionCodec.class);
//
//        conf.setClass("mapred.output.compression.codec", GzipCodec.class,CompressionCodec.class);
//        conf.set("mapred.output.compress", "true");

        Job job = new Job(conf,jobName);

        //Set Mapper and Reducer Classes
        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);

        //Set Map Output values.
        job.setMapOutputKeyClass(mapOutputKeyClass);
        job.setMapOutputValueClass(mapOutputValueClass);

        job.setOutputKeyClass(outputKeyClass);
        job.setOutputValueClass(outputValueClass);

        //Set the input and output.
        job.setInputFormatClass(inputFormatClass);
        job.setOutputFormatClass(outputFormatClass);

        //Delete the output path before run, to avoid exception
        FileSystem fs1 = FileSystem.get(conf);
        Path out1 = new Path(outputPath);
        fs1.delete(out1, true);

        //Set the input and output path
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setJarByClass(ArtistCoocurrence.class);

        job.waitForCompletion(true);
    }
}
