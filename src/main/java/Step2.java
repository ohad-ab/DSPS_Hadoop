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

public class Step2 {


public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
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
              String w1 = words[0];
              String w2 = words[1];
              String w3 = words[2];
              String w2w3 = w2 + "_" + w3;
              String w2w3w1w2w3 = w2w3 + "_" + entryWords;
              context.write(new Text(w2w3w1w2w3), new Text(splittedEntry[1] + "\t" + splittedEntry[2]));
              context.write(new Text(w1 + "*"), new Text(splittedEntry[3])); //w1 and occ
              context.write(new Text(w2 + "*"), new Text(splittedEntry[3])); //w2 and occ
              context.write(new Text(w3 + "*"), new Text(splittedEntry[3])); //w3 and occ
            }
        }
    }
}
 
  public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
      public static double log2(int x)
      {
          return (Math.log(x) / Math.log(2));
      }
      int C1 = 0;
      int N2 = 0;

      @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
        double currSum;
        double K2;
        double K3;
        double out;
//        Finds the C0 and saves it
        if (key.toString().equals("c0")){
            context.write(new Text("c0"), values.iterator().next());
        }
//        Finds only W2* (ends with * but not contains _)
       else if (!key.toString().contains("_") && key.toString().charAt(key.toString().length() - 1) == '*') {
           C1 = 0;
            for (Text value : values) {
                C1 += Integer.parseInt(value.toString());
            }
            context.write(key, new Text(Integer.toString(C1)));
        }
        else if(key.toString().charAt(key.toString().length() - 1) == '*'){
            N2 = 0;
            for (Text value : values) {
                N2 += Integer.parseInt(value.toString());
            }
        }

        else {
            String valuesStr = "";
            for (Text value : values) {
                valuesStr += value.toString();
            }
             String[] spliitedValues = valuesStr.split("\t");
             currSum = Double.parseDouble(spliitedValues[0]);
             K3 = Double.parseDouble(spliitedValues[1]);
             K2 = (log2(N2 + 1)+1) / (log2(N2 + 1)+2);
             out = currSum + (1-K3) * K2 * N2/C1;
             String[] splittedKey = key.toString().split("_");

//             Origin key is w2w3w1w2w3, we want to write only w1w2w3
             String w1w2w3 = splittedKey[2] + "_" + splittedKey[3] + "_" + splittedKey[4];
             context.write(new Text(w1w2w3), new Text(out + "\t" + K3 + "\t" + K2));
        }
    }
  }

    public static class CombinerClass extends Reducer<Text,Text,Text,Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
//        Finds the C0 and saves it
            if(key.toString().charAt(key.toString().length() - 1) == '*') {
                int Combined = 0;
                for (Text value : values) {
                    Combined += Integer.parseInt(value.toString());
                }
                context.write(key, new Text(Integer.toString(Combined)));
            }
            else{
                context.write(key, values.iterator().next());

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
    job.setJarByClass(Step2.class);
    job.setMapperClass(MapperClass.class);
    job.setPartitionerClass(PartitionerClass.class);
//    Combiner
    job.setCombinerClass(Step2.CombinerClass.class);
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