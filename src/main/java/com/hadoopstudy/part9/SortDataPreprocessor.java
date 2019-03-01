package com.hadoopstudy.part9;

import com.hadoopstudy.util.JobBuilder;
import com.hadoopstudy.util.NcdcRecordParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SortDataPreprocessor extends Configured implements Tool {

    static class CleanerMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
        private NcdcRecordParser parser = new NcdcRecordParser();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if(parser.isValidTemperature()){
                context.write(new IntWritable(parser.getAirTemperature()), value);
            }
        }
    }

    public int run(String[] args) throws Exception{
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if(job == null){
            return -1;
        }

        job.setMapperClass(CleanerMapper.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // 设置reduce任务数量为0
        job.setNumReduceTasks(0);

        // 设置输出格式 为 顺序文件
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // 按照压缩文件输出
        SequenceFileOutputFormat.setCompressOutput(job, true);

        // 设置压缩格式
        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        // 按照块 进行压缩
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        return job.waitForCompletion(true)? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new SortDataPreprocessor(), args);
        System.exit(exitCode);
    }
}
