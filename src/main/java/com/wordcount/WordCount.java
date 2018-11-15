package com.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program: hadoop_demo
 * @description:
 * @author: Mr.Walloce
 * @create: 2018/11/03 15:04
 **/
public class WordCount extends Configured implements Tool {

    /**
     * <LongWritable, Text, Text, IntWritable> 输入和输出的key-value类型
     */
    static class MyMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        //结果输出的字符串
        Text out_key = new Text();

        //结果输出的默认值
        IntWritable out_value = new IntWritable(1);

        /**
         * @param key     输入的字符串的偏移量
         * @param value   输入的字符串
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("map阶段开始执行，，，");
            String line = value.toString();
            long index = key.get();
            //对字符串进行处理，获取到单词
            String[] words = line.split(" ");
            if (words.length > 0) {
                for (String word : words) {
                    out_key.set(word);
                    context.write(out_key, out_value);
                }
            }
            System.out.println("map阶段结束。。。");
        }
    }

    /**
     * <Text, IntWritable, Text, IntWritable>输入和输出的key-value类型
     */
    static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("Reduce阶段开始执行...");
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            System.out.println("单词" + key.toString() + ":  " + result.get());
            context.write(key, result);
            System.out.println("Reduce阶段结束。。。");
        }
    }

    static class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("Combiner阶段开始...");
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result);
            System.out.println("Combiner阶段结束。。。");
        }
    }

    public int run(String[] args) throws Exception {

        //Hadoop的八股文
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());

        //************************对job进行具体的设置*************************
        //在集群中运行时不写会报错，本地运行科不写（最好写上）
        job.setJarByClass(WordCount.class);

        //设置输入输出路径
        Path in_path = new Path(args[0]);
        FileInputFormat.addInputPath(job, in_path);
        Path out_path = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, out_path);

        //输出前判断输出路径是否存在，存在则删除（输出路径不能重复）
        FileSystem fs = out_path.getFileSystem(conf);
        if (fs.exists(out_path)) {
            fs.delete(out_path, true);
        }

        //运行map类相关的参数设置
        job.setMapperClass(MyMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //运行Shuffle相关的参数设置
        job.setCombinerClass(MyCombiner.class);

        //设置reduce类相关的参数设置
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //运行是否成功
        boolean isSuccess = job.waitForCompletion(true);

        //运行成功返回0，反之返回1
        return isSuccess ? 0 : 1;
    }

    public static void main(String args[]) {
        System.out.println("Hello ");
        Configuration conf = new Configuration();

        args = new String[]{
                "hdfs://192.168.206.142:8020/walloce/testdata/test.txt",
                "hdfs://192.168.206.142:8020/walloce/output"
        };

        try {
            ToolRunner.run(conf, new WordCount(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
