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

public class NaiveBayesTrain{
    public static class TrainMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context)
                        throws IOException, InterruptedException
        {
            String[] tokens = value.toString().split(",");
            int index = tokens.length - 1;
            String theClass = tokens[index]; //
            for (int i=0; i<(index-1); i++) {
                Text reduceKey = new Text(tokens[i] + "," + theClass);  //
                context.write(reduceKey, one);
            }
            if(theClass.trim().equals("") || theClass==null){}
            else {
                Text reduceKey = new Text("CLASS," + theClass);
                context.write(reduceKey, one);
            }
        }
    }

    public static class TrainReducer extends Reducer<Text, IntWritable, Text, IntWritable>
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
            result.set(sum);
            context.write(key, result);
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
        FileSystem fs = FileSystem.get(conf);

        //Job job = new Job(conf,"Navie Bayes Train");
         Job job = Job.getInstance(conf, "Navie Bayes Train");

        job.setJarByClass(NaiveBayesTrain.class);
        job.setMapperClass(TrainMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(TrainReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.addInputPath(job, inputPath);
        if(fs.exists(outputPath)) fs.delete(outputPath, true);
        TextOutputFormat.setOutputPath(job, outputPath);

        fs.close();
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
