package cn.rabcheng;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @auther cheng
 * @create 2020-03-20 19:34
 */
public class WordcountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String s = value.toString();

        String[] split = s.split(" ");

        for(String word : split){

            k.set(word);
            context.write(k,v);

        }

    }
}
