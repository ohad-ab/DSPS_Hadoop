import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class Step4 {


public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

        while (itr.hasMoreTokens()) {
            String[] splittedEntry = itr.nextToken().split("\t");
            context.write(new Text(splittedEntry[0] + "_" + splittedEntry[1]), new Text(""));
        }
    }
}
 
  public static class ReducerClass extends Reducer<Text,Text,Text,Text> {

      @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            String[] splittedKey = key.toString().split("_");
            String w1w2w3 = splittedKey[0] + " " + splittedKey[1] + " " + splittedKey[2];
            context.write(new Text(w1w2w3), new Text(splittedKey[3]));

    }
  }
 
    public static class PartitionerClass extends Partitioner<Text, Text> {
      @Override
      public int getPartition(Text key, Text value, int numPartitions) {
          return key.hashCode() % numPartitions;
      }    
    }

    private static class CompareProbs extends WritableComparator {
        protected CompareProbs() {
            super(Text.class, true);
        }
        @Override
        public int compare(WritableComparable key1, WritableComparable key2) {
            String[] splittedKey1 = key1.toString().split("_");
            String key1_w1w2 = splittedKey1[0] + "_" + splittedKey1[1];
            double prob1 = Double.parseDouble(splittedKey1[3]);

            String[] splittedKey2 = key2.toString().split("_");
            String key2_w1w2 = splittedKey2[0] + "_" + splittedKey2[1];
            double prob2 = Double.parseDouble(splittedKey2[3]);

//          Check if the first two words are equals
//          If true: check the probability and set the highest probability to be first (-1)
            if (key1_w1w2.equals(key2_w1w2)){
                if(prob1 > prob2){
                    return -1;
                }
                else
                    return 1;
            }
//          If the first two words are not equal - do the normal comparison
            return (key1_w1w2.compareTo(key2_w1w2));

        }
    }

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "word count");
    job.setJarByClass(Step4.class);
    job.setMapperClass(MapperClass.class);
    job.setSortComparatorClass(Step4.CompareProbs.class);
    job.setPartitionerClass(PartitionerClass.class);
    job.setReducerClass(ReducerClass.class);
//    Map output
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
//    Job output
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
 
}