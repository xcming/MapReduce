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

public class DataTransform{
    public static class MyMapper extends Mapper<Object, Text, Text, Text>
    {

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
            if(token.length()>1)
                context.write(new Text(token.substring(0,tmp)), new Text(token.substring(tmp+1,token.length())));
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text>
    {


        public void reduce(Text key, Iterable<Text> values, Context context)
                            throws IOException, InterruptedException
        {
            for(Text val : values){
                context.write(key,val);
            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length!=2)
        {
            System.err.println("Usage: DataTransform <in> <out>");
            System.exit(2);
        }
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);

        //Job job = new Job(conf,"Navie Bayes Train");
         Job job = Job.getInstance(conf, "Data Transform");

        job.setJarByClass(DataTransform.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, inputPath);
        if(fs.exists(outputPath)) fs.delete(outputPath, true);
        TextOutputFormat.setOutputPath(job, outputPath);

        fs.close();
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
