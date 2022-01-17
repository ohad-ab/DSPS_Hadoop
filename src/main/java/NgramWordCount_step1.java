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

import static java.lang.Integer.parseInt;

public class NgramWordCount_step1 {


public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
//    private Text word = new Text();
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        int c0 = 0;
        while (itr.hasMoreTokens()) {
          String[] splittedEntry = itr.nextToken().split("\t");
          String entryWords = splittedEntry[0];
          IntWritable occ = new IntWritable(parseInt(splittedEntry[2]));
          Text w1 = new Text(entryWords.split(" ")[0]);
          Text w2 = new Text(entryWords.split(" ")[1]);
          Text w3 = new Text(entryWords.split(" ")[2]);
          Text w1w2 = new Text(w1 + "_" + w2 + "_*");
//          Text w2w3 = new Text(w2 + "_" + w3);
          Text w1w2w3 = new Text(w1 + "_" + w2 + "_" + w3);
//          context.write(w3, occ);
//          context.write(w2w3, occ);
          context.write(w1w2w3, occ);
          context.write(w1w2, occ);
          c0+=occ.get();
      }
     context.write(new Text("c0"),new IntWritable(c0));
    }
  }
 
  public static class ReducerClass extends Reducer<Text,IntWritable,Text,Text> {
      public static double log2(int x)
      {
          return (Math.log(x) / Math.log(2));
      }
    int c2 = 0;
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
        int N3 = 0;
        int c0 = 0;
        double k3;
        if(key.toString().equals("c0")) {
            for (IntWritable value : values) {
                c0 = c0 + value.get();
            }
//            MultipleOutputs outputs =new MultipleOutputs<Text,Text>(context);
//            outputs.write("c0", ,key,new Text(c0.toString()));
            context.write(key,new Text(Integer.toString(c0)));
        }
        else {
            for (IntWritable value : values) {
                N3 += value.get();
            }
//      String star = key.toString().substring(key.getLength()-1));
//      System.out.println("This is: " + star);
            //System.out.println(key.toString().charAt(key.toString().length() - 1));
            if (key.toString().charAt(key.toString().length() - 1) == '*') {
                c2 = N3;
                context.write(key,new Text(Integer.toString(N3)));
            }
            else {
//                k3 = (Math.log(N3 + 1) + 1) / (Math.log(N3 + 1) + 2);
                k3 = (log2(N3 + 1) + 1) / (log2(N3 + 1) + 2);
                double val = k3 * N3 / c2;
//                c0++;
                context.write(key, new Text(Double.toString(val)+"\t"+Double.toString(1-k3)));
            }
        }
    }
  }
 
    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
      @Override
      public int getPartition(Text key, IntWritable value, int numPartitions) {
          return key.hashCode() % numPartitions;
      }    
    }
 
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "word count");
    job.setJarByClass(NgramWordCount_step1.class);
    job.setMapperClass(MapperClass.class);
    job.setPartitionerClass(PartitionerClass.class);
    //job.setCombinerClass(ReducerClass.class);
    job.setReducerClass(ReducerClass.class);
//    Map output
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
//    Job output
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
 //   MultipleOutputs.addNamedOutput(job,"c0", SequenceFileOutputFormat.class,Text.class,Text.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
 
}