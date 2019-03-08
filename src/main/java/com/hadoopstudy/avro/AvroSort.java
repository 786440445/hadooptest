package com.hadoopstudy.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;

public class AvroSort extends Configured implements Tool {
    static class SortMapper extends Mapper<AvroKey<Integer>, NullWritable, AvroKey<Integer>, AvroValue<Integer>>{
        @Override
        protected void map(AvroKey<Integer> key, NullWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, new AvroValue<Integer>(key.datum()));
        }
    }
    static class SortReducer extends Reducer<AvroKey<Integer>, AvroValue<Integer>, AvroKey<Integer>, NullWritable>{
        @Override
        protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<Integer>> values, Context context) throws IOException, InterruptedException {
            for(AvroValue<Integer> value: values){
                context.write(new AvroKey<Integer>(value.datum()), NullWritable.get());
            }
        }
    }

    public int run(String[] args) throws Exception {
        if(args.length != 3){
            System.err.printf("Usage: %s [generic options] <input><output><schema-file>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        String input = args[0];
        String output = args[1];
        String schemaFile = args[2];

        Job job = Job.getInstance(getConf(), "Avro Sort");
        job.setJarByClass(getClass());
        // 这样用户提交job时通过-libjar设置的jar包就可以覆盖MR框架下面的包，这一点是很有用的，
        // 特别是对MR框架相关代码修改了之后，既不想替换集群的相关包，而又想要修改生效，这样一来就可以做到了。
        job.getConfiguration().setBoolean(Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        // 设置AvroJob的数据类型
        AvroJob.setDataModelClass(job, GenericData.class);
        // 创建模式对象
        Schema schema = new Schema.Parser().parse(new File(schemaFile));
        AvroJob.setInputKeySchema(job, schema);
        AvroJob.setMapOutputKeySchema(job, schema);
        AvroJob.setMapOutputValueSchema(job, schema);
        AvroJob.setOutputKeySchema(job, schema);

        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        job.setOutputKeyClass(AvroKey.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        return job.waitForCompletion(true)? 0: 1;
    }
    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new AvroSort(), args);
        System.exit(exitCode);
    }
}
