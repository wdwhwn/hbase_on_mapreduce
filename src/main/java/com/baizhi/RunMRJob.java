package com.baizhi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class RunMRJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1. 创建JOB对象
        Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM,"hadoop");
        Job job = Job.getInstance(configuration);
        job.setJarByClass(RunMRJob.class);

        //2. 初始化数据的输如输出格式
        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        //3. 初始化map的输入数据和reduce的计算输出
        TableMapReduceUtil.initTableMapperJob("spark1:t_user",
                new Scan(),
                MyMapper.class,
                Text.class,
                IntWritable.class,
                job);

        TableMapReduceUtil.initTableReducerJob("spark1:result",MyReducer.class,job);

        //4. 提交运行mr程序
        job.waitForCompletion(true);
    }
}
