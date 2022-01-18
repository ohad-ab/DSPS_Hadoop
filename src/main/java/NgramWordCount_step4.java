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

public class NgramWordCount_step4 {


public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
//    private Text word = new Text();
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        while (itr.hasMoreTokens()) {
            String[] splittedEntry = itr.nextToken().split("\t");
            context.write(new Text(splittedEntry[0]), new Text(splittedEntry[1]));
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
            context.write(key, new Text(values.iterator().next()));

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
    job.setJarByClass(NgramWordCount_step4.class);
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