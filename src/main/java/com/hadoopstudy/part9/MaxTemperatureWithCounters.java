package com.hadoopstudy.part9;

import com.hadoopstudy.util.NcdcRecordParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

/**
 * Created by Administrator on 2019/2/17.
 */
public class MaxTemperatureWithCounters extends Configured implements Tool {
    enum Temperature{
        MISSING,
        MALFORMED
    }

    static class MaxTemperatureMapperWithCounters extends Mapper<LongWritable, Text, Text, IntWritable>{
        private NcdcRecordParser parser = new NcdcRecordParser();
    }

    public int run(String[] strings) throws Exception {
        return 0;
    }
}
