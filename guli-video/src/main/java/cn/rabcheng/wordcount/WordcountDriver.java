package cn.rabcheng.wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @auther cheng
 * @create 2020-05-15 9:53
 */
public class WordcountDriver {

    public static void main(String[] args) throws IOException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(WordcountDriver.class);

    }
}
