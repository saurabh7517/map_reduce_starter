package org.dev;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by saurabh on 2/4/17.
 */
public class WordCountCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Integer sum = 0;
        for(IntWritable value : values){
            sum = sum + value.get();
        }
        context.write(key,new IntWritable(sum));
    }
}
