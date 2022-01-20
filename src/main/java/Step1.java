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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import static java.lang.Integer.parseInt;

public class Step1 {
public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {

    public boolean isRelevant(String[] entry) {
//      Check if it's triple
        if (entry.length != 3)
            return false;

//      Make sure word is not one char (also good for cleaning punctuation)
        if (entry[0].length() < 2 || entry[1].length() < 2 || entry[2].length() < 2)
            return false;

//        Check if letters are in Hebrew Alef-Bet
//        Unicode for Alef - 1488
//        Unicode for Tav - 1514
//        link - https://www.ssec.wisc.edu/~tomw/java/unicode.html#:~:text=1488-,HEBREW%20LETTER%20ALEF,-%D7%90
        char Alef = (char) 1488;
        char Tav = (char) 1514;
        for (String word : entry) {
            for (char c : word.toCharArray()) {
                if (c < Alef || c > Tav) {
                    return false;
                }
            }
        }
        return true;
    }


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
//      For our tests
//      StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        int c0 = 0;
//        while (itr.hasMoreTokens()) {
//          String[] splittedEntry = itr.nextToken().split("\t");
        String[] splittedEntry = value.toString().split("\t");
        String[] entryWords = splittedEntry[0].split(" ");
        IntWritable occ = new IntWritable(parseInt(splittedEntry[2]));
        if (isRelevant(entryWords)){
                Text w1 = new Text(entryWords[0]);
                Text w2 = new Text(entryWords[1]);
                Text w3 = new Text(entryWords[2]);
                Text w1w2 = new Text(w1 + "_" + w2 + "_*");
                Text w2w3 = new Text(w2 + "_" + w3 + "_*");
                Text w1w2w3 = new Text(w1 + "_" + w2 + "_" + w3);

                context.write(w1w2w3, occ); //N3
                context.write(w1w2, occ); //C2
                context.write(w2w3, occ); //Also for C2
                c0 += occ.get();
//      }
                context.write(new Text("c0"), new IntWritable(c0));
        }
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

            context.write(key,new Text(Integer.toString(c0)));
        }
        else {
            for (IntWritable value : values) {
                N3 += value.get();
            }

            if (key.toString().charAt(key.toString().length() - 1) == '*') {
                c2 = N3;
                context.write(key,new Text(Integer.toString(N3)));
            }
            else {
                k3 = (log2(N3 + 1) + 1) / (log2(N3 + 1) + 2);
                double val = k3 * N3 / c2;
                context.write(key, new Text(val +"\t"+ k3 +"\t"+ N3));
            }
        }
    }
  }

    public static class CombinerClass extends Reducer<Text,IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            System.out.println("-------- someone used me!!!! ----------");
            int N3 = 0;
            int c0 = 0;
            if(key.toString().equals("c0")) {
                for (IntWritable value : values) {
                    c0 = c0 + value.get();
                }
                context.write(key, new IntWritable(c0));
            }
            else {
                for (IntWritable value : values) {
                    N3 += value.get();
                }
                context.write(key,new IntWritable(N3));
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
    Job job = new Job(conf, "Triple count");
    job.setJarByClass(Step1.class);
    job.setMapperClass(MapperClass.class);
    job.setPartitionerClass(PartitionerClass.class);
//    Combiner
    job.setCombinerClass(CombinerClass.class);
    job.setReducerClass(ReducerClass.class);
//    Map output
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
//    Job output
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormatClass(SequenceFileInputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
 
}