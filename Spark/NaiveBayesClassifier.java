import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.io.DoubleWritable;

public class NaiveBayesClassifier implements java.io.Serializable {
    static List<Tuple2<PairOfStrings,DoubleWritable>>
        toWritableList(Map<Tuple2<String,String>,Double> PT){
        List<Tuple2<PairOfStrings,DoubleWritable>> list = new ArrayList<Tuple2<PairOfStrings,DoubleWritable>>();
        for (Map.Entry<Tuple2<String,String>,Double> entry : PT.entrySet()){
            list.add(new Tuple2<PairOfStrings,DoubleWritable>(
                new PairOfStrings(entry.getKey()._1,entry.getKey()._2),
                new DoubleWritable(entry.getValue())
            ));
        }
        return list;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
         System.err.println("Usage: NaiveBayesClassifier <input-data-filename> <PT-path> ");
         System.exit(1);
        }
        final String inputDataFilename = args[0];
        final String ProbTablePath = args[1];

        JavaSparkContext ctx = SparkUtil.createJavaSparkContext("naive-bayes-classifier");

        JavaRDD<String> train = ctx.textFile(inputDataFilename, 1);
        train.setAsTextFile("/output/1");
        long train_size = train.count();

        //map
        JavaPairRDD<Tuple2<String,String>, Integer> pairs =
            train.flatMapToPair(new PairFlatMapFunction<
            String,                   //A1 A2 ... Class
            Tuple2<String,String>,    //Tuple2(CLASS,class)  or Tuple2(Attr,Class)
            Integer                         // 1
            >(){
            public Iterable<Tuple2<Tuple2<String,String>,Integer>> call(String rec){
                List<Tuple2<Tuple2<String,String>,Integer>> result =
                    new ArrayList<Tuple2<Tuple2<String,String>,Integer>>();
                String[] token = rec.split("\t");
                int index = token.length - 1;
                String theClass = token[index];
                for (int i=0; i<index; i++) {
                    Tuple2<String,String> K = new Tuple2<String,String>(token[i],theClass);
                    result.add(new ArrayList<Tuple2<Tuple2<String,String>,Integer>>(K, 1));
                }
                Tuple2<String,String> K = new Tuple2<String,String>("CLASS",theClass);
                result.add(new ArrayList<Tuple2<Tuple2<String,String>,Integer>>(K, 1));
                return result;
            }
        });
        pairs.saveAsTextFile("/output/2");

        //reduce
        JavaPairRDD<Tuple2<String,String>, Integer> counts =
            pairs.refuceByKey(new Function2<Integer, Integer, Integer>(){
            public Integer call(Integer i1, Integer i2){
                return i1 + i2;
            }
            });
        counts.saveAsTextFile("/output/3");

        Map<Tuple2<String,String>, Integer> countsAsMap = counts.collectAsMap();

        //build PT, CT
        Map<Tuple2<String,String>, Double> PT = new HashMap<Tuple2<String,String>, Double>();
        List<String> CT = new ArrayList<String>();
        for(Map.entry<Tuple2<String,String>,Integer> entry : countsAsMap.entrySet()){
            Tuple2<String,String> k = entry.getKey();
            if(k._1.equals("CLASS")){
                PT.put(k, ((double) entry.getValue) / ((double) train_size) );
                CT.add(k._2);
            }
            else{
                Tuple2<String,String> k2 = new Tuple2<String,String>(k._1,k._2);;
                Integer c = countsAsMap.get(k2);
                if(c == null) PT.put(k, 0.0);
                else PT.put(k, ((double) entry.getValue) / ((double) c.intValue()) );
            }
        }

        List<Tuple2<PairOfStrings, DoubleWritable>> list = toWritableList(PT);
        JavaPairRDD<PairOfStrings, DoubleWritable> ptRDD = ctx.parallelizePairs(list);
        ptRDD.saveAsHadoopFile("naivebayesSpark/pt",
                            PairOfStrings.class,
                            DoubleWritable.class,
                            SequenceFileInputFormat.class
                            );
        JavaRDD<String> ctRDD = ctx.parallelize(CT);
        ctRDD.saveAsTextFile("/naivebayesSpark/classes");

    }

}
