package com.hadoopstudy.part9;

import com.hadoopstudy.util.JobBuilder;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MissingTemperatureFields extends Configured implements Tool {

    // 输入为job id
    // hadoop jar hadoop-example.jar MissingTemperatureFields job_1410450250506_0007
    public int run(String[] args) throws Exception{
        if(args.length != 1){
            JobBuilder.printUsage(this, "<job ID>");
            return -1;
        }
        String jobID = args[0];
        Cluster cluster = new Cluster(getConf());
        Job job = cluster.getJob(JobID.forName(jobID));
        if(job == null){
            System.out.printf("No Job with ID %s found.\n", jobID);
            return -1;
        }
        if(!job.isComplete()){
            System.err.printf("Job %s is not complete.\n", jobID);
            return  -1;
        }

        Counters counters = job.getCounters();
        // 计算 missing计数器的数量
        long missing = counters.findCounter(MaxTemperatureWithCounters.Temperature.MISSING).getValue();

        // 计算输入map记录 总数
        long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();

        System.out.printf("Records with missing temperature fields: %.2f%%\n", 100 * missing / total);

        return 0;

    }

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new MissingTemperatureFields(), args);
        System.exit(exitCode);
    }
}
