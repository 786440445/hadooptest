package com.hadoopstudy.part9;

import com.hadoopstudy.util.JobBuilder;
import com.hadoopstudy.util.NcdcRecordParser;
import com.hadoopstudy.util.TextPair;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

public class JoinRecordWithStationName extends Configured implements Tool{

    // 读取气象站数据Mapper
    static class JoinStationMapper extends Mapper<LongWritable, Text, TextPair, Text> {
        private NcdcRecordParser parser = new NcdcRecordParser();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            context.write(new TextPair(parser.getStationId(), "0"), new Text(parser.getStationName()));
        }
    }

    // 读取天气记录数据Mapper
    static class JoinRecordMapper extends Mapper<LongWritable, Text, TextPair, Text>{
        private NcdcRecordParser parser = new NcdcRecordParser();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            parser.parse(value);
            context.write(new TextPair(parser.getStationId(), "1"), value);
        }
    }

    static class JoinReducer extends Reducer<TextPair, Text, Text, Text> {
        @Override
        protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iter = values.iterator();
            Text stationName = new Text(iter.next());

            while(iter.hasNext()){
                Text record = iter.next();
                Text outValue = new Text(stationName.toString() + "\t" + record.toString());
                context.write(key.getFirst(), outValue);
            }
        }
    }

    // 分区 按照first相等 分到一个reducer中
    static class KeyPartitioner extends Partitioner<TextPair, Text>{
        public int getPartition(TextPair key, Text value, int numPartitions){
            return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public int run(String[] args) throws Exception{
        if(args.length != 3){
            JobBuilder.printUsage(this, "<ncdc input> <station input> <output>");
            return -1;
        }
        Job job = Job.getInstance(getConf(), "Join weather records with station names");
        job.setJarByClass(getClass());

        Path ncdcInputPath = new Path(args[0]);
        Path stationInputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        // 多输入
        MultipleInputs.addInputPath(job, ncdcInputPath, TextInputFormat.class, JoinRecordMapper.class);
        MultipleInputs.addInputPath(job, stationInputPath, TextInputFormat.class, JoinStationMapper.class);

        // 设置输出路径
        FileOutputFormat.setOutputPath(job, outputPath);

        // 设置分区
        job.setPartitionerClass(KeyPartitioner.class);

        // 设置组排序规则
        job.setGroupingComparatorClass(TextPair.FirstComparator.class);

        // Map输出key类型
        job.setMapOutputKeyClass(TextPair.class);

        job.setReducerClass(JoinReducer.class);

        // 输出key类型
        job.setOutputKeyClass(Text.class);

        return job.waitForCompletion(true)? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        String args1 = "hdfs://localhost:9000/matrix/input/stations.txt";
        String args2 = "hdfs://localhost:9000/matrix/input/records.txt";
        String args3 = "hdfs://localhost:9000/matrix/output1/";
        String[] arg = {args1, args2, args3};
        int exitCode = ToolRunner.run(new JoinRecordWithStationName(), arg);
        System.exit(exitCode);
    }
}