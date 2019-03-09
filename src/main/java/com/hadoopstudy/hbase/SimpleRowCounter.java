package com.hadoopstudy.hbase;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SimpleRowCounter extends Configured implements Tool{
    static class RowCounterMapper extends TableMapper<ImmutableBytesWritable, Result> {
        public static enum Counters{ROWS};
        public void map(ImmutableBytesWritable row, Result value, Mapper.Context context){
            context.getCounter(Counters.ROWS).increment(1);
        }
    }
    public int run(String[] args) throws Exception {
        if(args.length != 1){
            System.err.println("Usage: SimpleRowCounter <tablename>");
            return -1;
        }
        String tableName = args[0];
        // 创建扫描器
        Scan scan = new Scan();
        // 用每行的第一个单元格来填充result对象
        scan.setFilter(new FirstKeyOnlyFilter());

        Job job = Job.getInstance(getConf(), getClass().getSimpleName());
        job.setJarByClass(getClass());
        // 对作业进行配置
        TableMapReduceUtil.initTableMapperJob(tableName, scan, RowCounterMapper.class, ImmutableBytesWritable.class, Result.class, job);

        job.setNumReduceTasks(0);

        job.setOutputFormatClass(NullOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] strings) throws Exception {
        String[] args = {"test"};
        int exitCode = ToolRunner.run(HBaseConfiguration.create(), new SimpleRowCounter(), args);
        System.exit(exitCode);
    }
}