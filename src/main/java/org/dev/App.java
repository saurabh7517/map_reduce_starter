package org.dev;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App{
    Configuration configuration;


    public static void main(String[] args ) {
        int exitCode = 0;
        Job job = null;
        try {
            Configuration conf = new Configuration();
            job = new Job(conf,"WordCountJob");
            job.setJarByClass(App.class);
            job.setJobName("MapReduce After a Long Time");


            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReducer.class);

            FileInputFormat.addInputPath(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[1]));

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

//            job.setNumReduceTasks(0);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            exitCode = job.waitForCompletion(true) ? 0 : 1;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }
    }


}
