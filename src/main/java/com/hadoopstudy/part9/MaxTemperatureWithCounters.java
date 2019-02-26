package com.hadoopstudy.part9;

import com.hadoopstudy.util.JobBuilder;
import com.hadoopstudy.util.NcdcRecordParser;
import com.hadoopstudy.v2.MaxTemperature.MaxTemperatureReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

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

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if(parser.isValidTemperature()){
                int airTemperate = parser.getAirTemperature();
                context.write(new Text(parser.getYear()), new IntWritable(airTemperate));
            }else if(parser.isMalformedTemperate())
            {
                System.out.println("Ignoring possibly corrupt input: " + value);
                context.getCounter(Temperature.MALFORMED).increment(1);
            }else if(parser.isMissingTemperature()){
                context.getCounter(Temperature.MISSING).increment(1);
            }
            context.getCounter("TemperatureQuality", parser.getQuality()).increment(1);
        }
    }

    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if(job == null)
        {
            return -1;
        }
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(MaxTemperatureMapperWithCounters.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        return job.waitForCompletion(true)? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new MaxTemperatureWithCounters(), args);
        System.exit(exitCode);
    }
}
