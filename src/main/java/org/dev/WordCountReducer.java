package org.dev;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by saurabh on 3/31/2017.
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Integer sum = 0;
        for(IntWritable value : values){
            sum = sum + value.get();
        }
        context.write(key,new IntWritable(sum));
    }
}
