package org.dev;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by saurabh on 3/31/2017.
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    private final static IntWritable val = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String wordArray[] = line.split(" ");
        for(int i =0 ; i < wordArray.length ; i++){
            context.write(new Text(wordArray[i]),val);
        }
    }
}
