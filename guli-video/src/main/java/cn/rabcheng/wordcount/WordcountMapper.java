package cn.rabcheng.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @auther cheng
 * @create 2020-05-15 9:52
 */
public class WordcountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    IntWritable  v = new IntWritable(1);
    Text t = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String s = value.toString();

        String[] split = s.split(",");

        for (String s1 : split) {
            t.set(s1);
            context.write(t,v);
        }
    }
}
