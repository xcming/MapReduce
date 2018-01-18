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

public class NaiveBayesTrainTest{
    public static class TestMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context)
                        throws IOException, InterruptedException
        {
            String token = value.toString();
            int tmp = 0;
            for (int i=token.length()-1; i>=0; i--) {
                if(token.charAt(i)==','){
                    tmp=i;
                    break;
                }
            }
            context.write(new Text(token.substring(0,tmp)), one);
        }
    }

    public static class TestReducer extends Reducer<Text, IntWritable, Text, Text>
    {
        public Map<String, Double> ProTable = new HashMap<String, Double>();//store probability table

        public void setup(Context context) throws IOException{
            Configuration conf = context.getConfiguration();
            Path ProPath = new Path(conf.get("ProPath"));
            FileSystem fs = ProPath.getFileSystem(conf);

            FileStatus[] stats=fs.listStatus(ProPath);
            for(int i=0;i<stats.length;i++){
                if(stats[i].isFile()){
                    FSDataInputStream infs=fs.open(stats[i].getPath());
                    LineReader reader=new LineReader(infs,conf);
                    Text line=new Text();

                    while(reader.readLine(line)>0){
                        String[] temp=line.toString().split("\t");
                        //System.out.println(temp.length);
                        ProTable.put(temp[0],Double.parseDouble(temp[1]));
                    }
                    reader.close();
                }
            }

        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                            throws IOException, InterruptedException
        {
                String[] attribute = key.toString().split(",");
                double pro = 1.0,pro2 = 1.0;
                pro = pro * ProTable.get(" >50K,CLASS");
                for(String attr : attribute){
                    String test = attr + ", >50K";
                    if(ProTable.get(test)!=null) pro = pro * ProTable.get(test);
                }
                pro2 = pro2 * ProTable.get(" <=50K,CLASS");
                for(String attr : attribute){
                    String test = attr + ", <=50K";
                    if(ProTable.get(test)!=null) pro2 = pro2 * ProTable.get(test);
                }
                if(pro>pro2) context.write(key,new Text(">50K"));
                else context.write(key,new Text("<=50K"));

        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length!=2)
        {
            System.err.println("Usage: NaiveBayesTrain <in> <out>");
            System.exit(2);
        }
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        String ProPath = "hdfs://192.168.142.128:9000/naive_bayes/probality_output";
        conf.set("ProPath", ProPath);

        FileSystem fs = FileSystem.get(conf);

        //Job job = new Job(conf,"Navie Bayes Train");
         Job job = Job.getInstance(conf, "Navie Bayes Train");

        job.setJarByClass(NaiveBayesTrainTest.class);
        job.setMapperClass(TestMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(TestReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, inputPath);
        if(fs.exists(outputPath)) fs.delete(outputPath, true);
        TextOutputFormat.setOutputPath(job, outputPath);

        fs.close();
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
