import java.util.Scanner;
import java.io.IOException;
import java.util.ArrayList;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
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

public class NaiveBayesPro{
    public static class ProMapper extends Mapper<Object, Text, Text, Text>
    {

        public void map(Object key, Text value, Context context)
                        throws IOException, InterruptedException
        {
            String[] token = value.toString().split("\t");  //CLASS  >50K  num  ||   Male  attr10  >50K  num

            if(token.length>1 && token[0].equals("CLASS")){  //如果是分类
                context.write(new Text(token[0]), new Text(token[1]+"\t"+token[2])); // key:CLASS value:>50K   num
            }
            else if(token.length>1){    //如果是属性
                context.write(new Text(token[2]), new Text(token[0]+"\t"+token[1]+"\t"+token[3]));
                //key:>50K   value: Male   attr10   num
            }
        }
    }

    public static class ProReducer extends Reducer<Text, Text, Text, DoubleWritable>
    {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context)
                            throws IOException, InterruptedException
        {
            Map<String, Integer> map = new HashMap<String, Integer>();
            int sum = 0;
            if(key.toString().equals("CLASS")){
                for(Text val : values){
                    String[] token = val.toString().split("\t");
                    Integer num = Integer.parseInt(token[1]);
                    sum += num;
                    map.put(key.toString()+"\t"+token[0], num);   //key: CLASS >50K     value: num
                }
            }
            else{
                for(Text val : values){
                    String[] token = val.toString().split("\t");
                    if(token.length>1){
                        Integer num = Integer.parseInt(token[2]);
                        sum += num;
                        map.put(token[0]+"\t"+token[1]+"\t"+key, num);  //key: Male attr10 >50K     value: num
                    }
                }

            }
            double probability = 1.0;
            for(Map.Entry<String,Integer> entry:map.entrySet()){
                probability = entry.getValue()*1.0/sum;   //如果是分类，则是计算先验概率； 如果是属性，则是计算条件概率
                context.write(new Text(entry.getKey()),new DoubleWritable(probability));
            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length!=2)
        {
            System.err.println("Usage: NaiveBayesPro <in> <out>");
            System.exit(2);
        }
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);

        //Job job = new Job(conf,"Navie Bayes Train");
         Job job = Job.getInstance(conf, "NaiveBayes Probability Table");

        job.setJarByClass(NaiveBayesPro.class);
        job.setMapperClass(ProMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ProReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        TextInputFormat.addInputPath(job, inputPath);
        if(fs.exists(outputPath)) fs.delete(outputPath, true);   //若输出路径已存在，删除该路径
        TextOutputFormat.setOutputPath(job, outputPath);

        fs.close();
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
