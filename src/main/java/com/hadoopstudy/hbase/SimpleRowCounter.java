package com.hadoopstudy.hbase;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

public class SimpleRowCounter extends Configured implements Tool{
    static class RowCounterMapper extends TableMapper<ImmutableBytesWritable, Result>{
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
        return 0;
    }
}