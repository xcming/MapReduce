import java.util.Scanner;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class NaiveBayesCount{
    public static class CountMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context)
                        throws IOException, InterruptedException
        {
            String[] token = value.toString().split("\t");
            int index = token.length - 1;
            context.write(new Text(token[index]), one);    //key: true || false   vlaue: 1
        }
    }

    public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                            throws IOException, InterruptedException
        {
            int sum = 0;
            for(IntWritable val : values)
            {
                sum += val.get();
            }
            result.set(sum);    //统计true和false的个数
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length!=2)
        {
            System.err.println("Usage: NaiveBayesCount <in> <out>");
            System.exit(2);
        }
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);

        //Job job = new Job(conf,"Navie Bayes Train");
         Job job = Job.getInstance(conf, "Navie Bayes Count");

        job.setJarByClass(NaiveBayesCount.class);
        job.setMapperClass(CountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.addInputPath(job, inputPath);
        if(fs.exists(outputPath)) fs.delete(outputPath, true);    //若输出路径已存在，删除该路径
        TextOutputFormat.setOutputPath(job, outputPath);

        fs.close();
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
