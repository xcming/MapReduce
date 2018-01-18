import java.util.Scanner;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.*;
import java.lang.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class NaiveBayesVerify{
    public static class VerifyMapper extends Mapper<Object, Text, Text, Text>
    {

        public void map(Object key, Text value, Context context)
                        throws IOException, InterruptedException
        {
            String[] token = value.toString().split("\t");
            int index = token.length - 1;
            if(index>0){
                StringBuilder outkey = new StringBuilder("");
                for (int i=0; i<index-1; i++) {
                    outkey.append(token[i]+"\t");
                }
                outkey.append(token[index-1]);
                context.write(new Text(outkey.toString()), new Text(token[index])); //key: 所有属性  value:分类

            }
        }
    }

    public static class VerifyReducer extends Reducer<Text, Text, Text, Text>
    {

        public void reduce(Text key, Iterable<Text> values, Context context)
                            throws IOException, InterruptedException
        {
                Set<String> set = new HashSet<String>();
                for(Text val : values){
                    set.add(val.toString());
                }
                if(set.size()==1)  context.write(key,new Text("true"));    //如果set中只有一个值，即预测和分类一样，则判断为true
                else context.write(key,new Text("false"));                 //如果set中有两个值，即预测和分类不一样，则判断为false

        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length!=2)
        {
            System.err.println("Usage: NaiveBayesVerify <in> <out>");
            System.exit(2);
        }
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileSystem fs = FileSystem.get(conf);

        //Job job = new Job(conf,"Navie Bayes Train");
         Job job = Job.getInstance(conf, "Navie Bayes Verify");

        job.setJarByClass(NaiveBayesVerify.class);
        job.setMapperClass(VerifyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(VerifyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, inputPath);
        if(fs.exists(outputPath)) fs.delete(outputPath, true);   //若输出路径已存在，删除该路径
        TextOutputFormat.setOutputPath(job, outputPath);

        fs.close();
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
