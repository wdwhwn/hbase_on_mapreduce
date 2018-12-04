package com.baizhi;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class MyReducer extends TableReducer<Text, IntWritable, NullWritable> { // 等价于 null

    /**
     *
     * @param key  输入的key  age
     * @param values  输入的value  [1,2,3,4,5.....999]
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;
        for (IntWritable value : values) {
            sum += value.get();
            count ++;
        }

        double avg = sum / count;
        // reduce计算结果输出到 HBase
//          rowkey
        Put put = new Put("age".getBytes());
//        在rowkey为age 的列簇cf1下添加avg字段
        put.addColumn("cf1".getBytes(),"avg".getBytes(), Bytes.toBytes(avg));

        context.write(null,put);
    }
}
