package cn.rabcheng.submit;

import cn.rabcheng.mapper.VideoETLMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @auther cheng
 * @create 2020-03-24 13:03
 */
public class VideoETLRunner implements Tool {

    private Configuration configuration;

    public int run(String[] strings) throws Exception {

        //1.获取job对象
        Job job = Job.getInstance(configuration);

        //2.设置jar包路径
        job.setJarByClass(VideoETLRunner.class);

        //3.设置mapper类&kv输出类型
        job.setMapperClass(VideoETLMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        //4.设置最终的kv输出类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //5.设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        job.setNumReduceTasks(0);

        //6.提交任务
        boolean res = job.waitForCompletion(true);


        return res ? 0 : 1;
    }

    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConf() {
        return this.configuration;
    }

    public static void main(String[] args) {
        try {
            int run = ToolRunner.run(new VideoETLRunner(), args);
            System.out.println(run);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
