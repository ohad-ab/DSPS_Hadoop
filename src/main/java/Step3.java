import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class NgramWordCount_step3 {


public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
//    private Text word = new Text();
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        while (itr.hasMoreTokens()) {
            String[] splittedEntry = itr.nextToken().split("\t");
            String entryWords = splittedEntry[0];
            if (entryWords.equals("c0")){
              context.write(new Text("c0"), new Text(splittedEntry[1]));
            }
            else if (entryWords.charAt(entryWords.length() - 1) == '*'){
              context.write(new Text(entryWords), new Text(splittedEntry[1]));
            }
            else{
              String[] words = entryWords.split("_");
//              String w1 = words[0];
              String w2 = words[1];
              String w3 = words[2];
//              String w2w3 = w2 + "_" + w3;
              String w3w1w2w3 = w3 + "_" + entryWords;
              context.write(new Text(w3w1w2w3), new Text(splittedEntry[1] + "\t" + splittedEntry[2]+ "\t" + splittedEntry[3]));
//              context.write(new Text(w1 + "*"), new Text(splittedEntry[3])); //w1 and occ
//              context.write(new Text(w2 + "*"), new Text(splittedEntry[3])); //w2 and occ
//              context.write(new Text(w3 + "*"), new Text(splittedEntry[3])); //w3 and occ
            }
        }
    }
}
 
  public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
      public static double log2(int x)
      {
          return (Math.log(x) / Math.log(2));
      }
      int C0 = 0;
      int N1 = 0;

      @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
        double currSum;
        double K2;
        double K3;
        double out;
//        Finds the C0 and saves it
        if (key.toString().equals("c0")){
            C0 = Integer.parseInt(values.iterator().next().toString());
        }
//        Finds only W2* (ends with * but not contains _)
       else if (key.toString().charAt(key.toString().length() - 1) == '*') {
            N1 = Integer.parseInt(values.iterator().next().toString());
        }

        else {
            String valuesStr = "";
            for (Text value : values) {
                valuesStr += value.toString();
            }
             String[] spliitedValues = valuesStr.split("\t");
             currSum = Double.parseDouble(spliitedValues[0]);
             K3 = Double.parseDouble(spliitedValues[1]);
             K2 = Double.parseDouble(spliitedValues[2]);
             out = currSum + (1-K3) * (1-K2) * N1/C0;
             String[] splittedKey = key.toString().split("_");

//             Origin key is w2w3w1w2w3, we want to write only w1w2w3
             String w1w2w3 = splittedKey[1] + "_" + splittedKey[2] + "_" + splittedKey[3];
             context.write(new Text(w1w2w3), new Text(Double.toString(out)));
        }
    }
  }
 
    public static class PartitionerClass extends Partitioner<Text, Text> {
      @Override
      public int getPartition(Text key, Text value, int numPartitions) {
          return key.hashCode() % numPartitions;
      }    
    }
 
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "word count");
    job.setJarByClass(NgramWordCount_step3.class);
    job.setMapperClass(MapperClass.class);
    job.setPartitionerClass(PartitionerClass.class);
    //job.setCombinerClass(ReducerClass.class);
    job.setReducerClass(ReducerClass.class);
//    Map output
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
//    Job output
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
 //   MultipleOutputs.addNamedOutput(job,"c0", SequenceFileOutputFormat.class,Text.class,Text.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
 
}