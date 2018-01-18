import java.util.Scanner;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

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

public class NaiveBayesTrainPro{
    public static class ProMapper extends Mapper<Object, Text, Text, Text>
    {


        public void map(Object key, Text value, Context context)
                        throws IOException, InterruptedException
        {
            String[] tokens = value.toString().split("\t");
            String[] attribute = tokens[0].split(",");
            if(attribute[0].equals("CLASS")){
                Text reduceKey = new Text(attribute[0]);
                Text reduceValue = new Text(attribute[1]+","+tokens[1]);
                context.write(reduceKey, reduceValue);
            }
            else{
                Text reduceKey = new Text(attribute[1]);
                Text reduceValue = new Text(attribute[0]+","+tokens[1]);
                context.write(reduceKey, reduceValue);
            }


        }
    }

    public static class ProReducer extends Reducer<Text, Text, Text, DoubleWritable>
    {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context)
                            throws IOException, InterruptedException
        {
            int sum = 0;
            Map<String,Integer> map = new HashMap<String,Integer>();
            for (Text val : values)
            {
                String[] tmp = val.toString().split(",");
                String k = tmp[0] + "," + key;
                Integer v = Integer.parseInt(tmp[1]);
                map.put(k,v);
                sum += v;
            }
            double probability=0.0;
            for(Map.Entry<String,Integer> entry:map.entrySet()){
                //probability = (sum+1)*1.0/(entry.getValue()+count);//
                probability = entry.getValue()*1.0/sum;
                context.write(new Text(entry.getKey()), new DoubleWritable(probability));
            }

        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length!=2)
        {
            System.err.println("Usage: NaiveBayesTrainPro <in> <out>");
            System.exit(2);
        }
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);

        //Job job = new Job(conf,"Navie Bayes Train");
         Job job = Job.getInstance(conf, "Navie Bayes Train Probility");

        job.setJarByClass(NaiveBayesTrainPro.class);
        job.setMapperClass(ProMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ProReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        TextInputFormat.addInputPath(job, inputPath);
        if(fs.exists(outputPath)) fs.delete(outputPath, true);
        TextOutputFormat.setOutputPath(job, outputPath);

        fs.close();
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
