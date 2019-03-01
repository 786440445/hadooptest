//package com.hadoopstudy.part9;
//
//import com.hadoopstudy.util.IntPair;
//import com.hadoopstudy.util.JobBuilder;
//import com.hadoopstudy.util.NcdcRecordParser;
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.WritableComparator;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Partitioner;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//
//import java.io.IOException;
//import java.util.Iterator;
//
//public class MaxTemperatureUsingSecondarySort extends Configured implements Tool {
//
//    // Mapper 实现
//    static class MaxTemperatureMapper extends Mapper<LongWritable, Text, IntPair, NullWritable>{
//        private NcdcRecordParser parser = new NcdcRecordParser();
//
//        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
//            parser.parse(value);
//            if(parser.isValidTemperature()){
//                context.write(new IntPair(parser.getYear(), parser.getAirTemperature()), NullWritable.get());
//            }
//        }
//    }
//
//    // Reducer 实现
//    static class MaxTemperatureReducer extends Reducer<IntPair, NullWritable, IntPair, NullWritable>{
//        protected void reduce(IntPair key, Iterator<NullWritable> values, Context context) throws IOException, InterruptedException {
//            context.write(key, NullWritable.get());
//        }
//    }
//
//    // 设置分区规则
//    public static class FirstPartitioner extends Partitioner<IntPair, NullWritable> {
//        @Override
//        public int getPartition(IntPair key, NullWritable value, int numPartitions) {
//            return Math.abs(key.getFirst() * 127) % numPartitions;
//        }
//    }
//
//    // 按照key value排序规则
//    public static class KeyComparator extends WritableComparator{
//        protected KeyComparator(){
//            super(IntPair.class, true);
//        }
//        public int compare(WritableComparator w1, WritableComparator w2){
//            IntPair ip1 = (IntPair) w1;
//            IntPair ip2 = (IntPair) w2;
//            int cmp = IntPair.compare(ip1.getFirst(), ip2.getFirst());
//            if(cmp != 0){
//                return cmp;
//            }
//            return -IntPair.compare(ip1.getSecond(), ip2.getSecond());
//        }
//    }
//
//    // 按照组进行排序规则
//    public static class GroupComparator extends WritableComparator{
//        protected GroupComparator(){
//            super(IntPair.class, true);
//        }
//        public int compare(WritableComparator w1, WritableComparator w2){
//            IntPair ip1 = (IntPair) w1;
//            IntPair ip2 = (IntPair) w2;
//            return IntPair.compare(ip1.getFirst(), ip2.getFirst());
//        }
//    }
//
//    public int run(String[] args) throws Exception{
//        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
//        if(job == null){
//            return -1;
//        }
//
//        job.setMapperClass(MaxTemperatureMapper.class);
//        job.setPartitionerClass(FirstPartitioner.class);
//        job.setSortComparatorClass(KeyComparator.class);
//        job.setGroupingComparatorClass(GroupComparator.class);
//
//        job.setReducerClass(MaxTemperatureReducer.class);
//        job.setOutputKeyClass(IntPair.class);
//        job.setOutputValueClass(NullWritable.class);
//
//        return job.waitForCompletion(true)? 0 : 1;
//    }
//
//    public static void main(String[] args) throws Exception{
//        int exitCode = ToolRunner.run(new MaxTemperatureUsingSecondarySort(), args);
//        System.exit(exitCode);
//    }
//}
