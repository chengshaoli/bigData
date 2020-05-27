package cn.rabcheng.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @auther cheng
 * @create 2020-05-15 9:52
 */
public class WordcountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

    int sum;
    IntWritable t = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {


        for (IntWritable value : values) {
            sum = 0;
            sum+= value.get();
        }

        t.set(sum);
        context.write(key,t);
    }
}
