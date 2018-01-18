import java.util.Scanner;
import java.io.IOException;
import java.util.*;
import java.lang.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.util.GenericOptionsParser;

public class NaiveBayesTest{
    public static class TestMapper extends Mapper<Object, Text, Text, Text>
    {
        public void map(Object key, Text value, Context context)
                        throws IOException, InterruptedException
        {
            String[] token = value.toString().split("\t");
            int index = token.length - 1;
            StringBuilder outkey = new StringBuilder("");
            for (int i=0; i<index-1; i++) {
                outkey.append(token[i]+"\t");
            }
            outkey.append(token[index-1]);   //不同属性之间以制表符分隔，取出真实的分类
            context.write(new Text(outkey.toString()), new Text(token[index]));
        }
    }

    public static class TestReducer extends Reducer<Text, Text, Text, Text>
    {
        Map<String, Double> ProTable = new HashMap<String,Double>();  //存放概率表
        ArrayList<String> classes = new ArrayList<String>();          //存放类别

        public void setup(Context context) throws IOException{
            Configuration conf = context.getConfiguration();
            Path ProPath = new Path(conf.get("ProPath"));
            Path ClassPath = new Path(conf.get("ClassPath"));
            FileSystem fs = ProPath.getFileSystem(conf);
            // 读取之前生成的概率表，并放入ProTable中
            FileStatus[] stats=fs.listStatus(ProPath);
            for(int i=0;i<stats.length;i++){
                if(stats[i].isFile()){
                    FSDataInputStream infs=fs.open(stats[i].getPath());
                    LineReader reader=new LineReader(infs,conf);
                    Text line=new Text();
                    while(reader.readLine(line)>0){
                        String[] token=line.toString().split("\t");
                        int index = token.length - 1;
                        StringBuilder outkey = new StringBuilder("");
                        for (int j=0; j<index-1; j++) {
                            outkey.append(token[j]+"\t");
                        }
                        outkey.append(token[index-1]);
                        ProTable.put(outkey.toString(),Double.parseDouble(token[index]));
                    }
                    reader.close();
                }
            }
            // 读取之前生成的类别表，并放入classes中
            FileStatus[] stats2=fs.listStatus(ClassPath);
            for(int i=0;i<stats2.length;i++){
                if(stats2[i].isFile()){
                    FSDataInputStream infs=fs.open(stats2[i].getPath());
                    LineReader reader=new LineReader(infs,conf);
                    Text line=new Text();
                    while(reader.readLine(line)>0){
                        String[] temp=line.toString().split("\t");
                        classes.add(temp[1]);
                    }
                    reader.close();
                }
            }

        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                            throws IOException, InterruptedException
        {
            double pro = 0.0;
            String result = "";
            for(int i=0;i<classes.size();i++){    //遍历所有分类
                double now_pro = 1.0;
                String from = (String)classes.get(i);
                String[] attr = key.toString().split("\t");
                now_pro = now_pro * ProTable.get("CLASS\t"+from);   //乘现在类别下的先验概率
                for(int j=0;j<attr.length;j++){   //遍历所有属性
                    String testkey = attr[j]+"\tattr"+j+"\t"+from;
                    if(ProTable.get(testkey)!=null) now_pro = now_pro * ProTable.get(testkey);  //乘现在类别下的条件概率
                }
                if(now_pro > pro){//取最大概率类别作为预测分类
                    result = from;
                    pro = now_pro;
                }
            }
            for (Text val : values){  // 判断预测结果是否正确，若正确，输出true，否则，输出false
                if(result.equals(val.toString())){
                    context.write(key, new Text("true"));
                }
                else context.write(key, new Text("false"));
            }

        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length!=2)
        {
            System.err.println("Usage: NaiveBayesTest <in> <out>");
            System.exit(2);
        }
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);

        String ProPath = "hdfs://192.168.142.128:9000/naivebayes/Pro_output";              //设置概率表输入路径
        String ClassPath = "hdfs://192.168.142.128:9000/naivebayes/Classify/class_output"; //设置类别表输入路径
        conf.set("ProPath", ProPath);
        conf.set("ClassPath", ClassPath);


        //Job job = new Job(conf,"Navie Bayes Train");
         Job job = Job.getInstance(conf, "NaiveBayes Test");

        job.setJarByClass(NaiveBayesTest.class);
        job.setMapperClass(TestMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(TestReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, inputPath);
        if(fs.exists(outputPath)) fs.delete(outputPath, true);   //若输出路径已存在，删除该路径
        TextOutputFormat.setOutputPath(job, outputPath);

        fs.close();
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
