package com.hadoopstudy.avro;

import com.hadoopstudy.util.NcdcRecordParser;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class AvroGenericMaxTemperature extends Configured implements Tool {
    private static final Schema SCHEMA = new Schema.Parser().parse(
            "{" +
                    "\"type\" :\" record\"," +
                    "\"name\" :\" WeatherRecord\"," +
                    "\"doc\" :\"A weather reading.\"," +
                    "\"fields\": [" +
                        "{\"name\": \"year\", \"type\": \"int\"}," +
                        "{\"name\": \"temperature\", \"type\": \"int\"}," +
                        "{\"name\": \"stationId\", \"type\": \"string\"}," +
                    "]" +
                "}"
    );

    public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, AvroKey<Integer>, AvroValue<GenericRecord>> {
        private NcdcRecordParser parser = new NcdcRecordParser();
        // 创建模式对应的记录对象
        private GenericRecord record = new GenericData.Record(SCHEMA);

        // map端输出以AvroKey<Integer>作为key，以AvroValue<GenericRecord>作为value
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            parser.parse(value.toString());
            if(parser.isValidTemperature()){
                record.put("year", Integer.parseInt(parser.getYear()));
                record.put("temperature", parser.getAirTemperature());
                record.put("stationId", parser.getStationId());

                context.write(new AvroKey<Integer>(Integer.parseInt(parser.getYear())), new AvroValue<GenericRecord>(record));
            }
        }
    }

    public static class MaxTemperatureReducer extends Reducer<AvroKey<Integer>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> {
        // reduce端  输出同一个Key中的AvroValue<GenericRecord>列表中的最大项的key
        @Override
        protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException {
            GenericRecord max = null;
            for(AvroValue<GenericRecord> value: values){
                GenericRecord record = value.datum();
                if(max == null || (Integer)record.get("temperature") > (Integer)max.get("temperature")){
                    max = newWeatherRecord(record);
                }
            }
            context.write(new AvroKey<GenericRecord>(max), NullWritable.get());
        }
        private GenericRecord newWeatherRecord(GenericRecord value){
            GenericRecord record = new GenericData.Record(SCHEMA);
            record.put("year", value.get("year"));
            record.put("temperature", value.get("temperature"));
            record.put("stationId", value.get("stationId"));
            return record;
        }
    }

    public int run(String[] args) throws Exception {
        if(args.length != 2){
            System.out.printf("Usage: %s [generic options] <input><output>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        Job job = Job.getInstance(getConf(), "Max Termperature");
        job.setJarByClass(getClass());
        //这样用户提交job时通过-libjar设置的jar包就可以覆盖MR框架下面的包，这一点是很有用的，
        // 特别是对MR框架相关代码修改了之后，既不想替换集群的相关包，而又想要修改生效，这样一来就可以做到了。
        job.getConfiguration().setBoolean(Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setMapOutputValueSchema(job, SCHEMA);
        AvroJob.setOutputKeySchema(job, SCHEMA);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new AvroGenericMaxTemperature(), args);
        System.exit(exitCode);
    }
}
