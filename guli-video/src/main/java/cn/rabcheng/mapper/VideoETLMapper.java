package cn.rabcheng.mapper;

import cn.rabcheng.ETL.ETLUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @auther cheng
 * @create 2020-03-24 13:03
 */
public class VideoETLMapper extends Mapper<LongWritable,Text,NullWritable,Text>{

    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String etlString = ETLUtil.oriString2ETLString(line);

        if(StringUtils.isEmpty(etlString)) return;

        v.set(etlString);

        context.write(NullWritable.get(),v);

    }
}
