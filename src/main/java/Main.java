//import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.jets3t.service.security.AWSCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.EmrClientBuilder;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.emr.model.*;

import java.io.IOException;


public class Main {
    public static AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
    static String bucketName = "oo-dspsp-ass2";
    //        Link to Google hebrew 3-Grams in S3
    static String nGramsPath = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";

    public static void main(String[] args){
        FromTask("WordCount.jar");
    }

    public static void FromTask(String jarPath){
//        AWSCredentials credentials = new AWSCredentials;
        EmrClient mapReduce = EmrClient.builder().region(Region.US_EAST_1).build();
        HadoopJarStepConfig hadoopJarStep = HadoopJarStepConfig.builder()
                .jar(jarPath)
                .mainClass("some.pack.MainClass")
                .args("s3n://"+bucketName+"/input/", "s3n://"+bucketName+"/output/")
                .build();
        StepConfig stepConfig = StepConfig.builder()
                .name("stepname")
                .hadoopJarStep(hadoopJarStep)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
        JobFlowInstancesConfig instances = JobFlowInstancesConfig.builder()
                .instanceCount(2)

                .masterInstanceType("m4.large") //TODO: check what is the free one
                .slaveInstanceType("m4.large")
                .hadoopVersion("2.6.0").ec2KeyName("key1")
                .keepJobFlowAliveWhenNoSteps(false)
                .build();
//                .placement(PlacementType.builder().region(Region.US_EAST_1).build()); //TODO: check if needed
        RunJobFlowRequest runFlowRequest = RunJobFlowRequest.builder()
                .name("jobname")
                .instances(instances)
                .steps(stepConfig)
                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("EMR_EC2_DefaultRole")
                .logUri("s3n://" + bucketName + "/logs/")
                .build();
        RunJobFlowResponse runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.jobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
    public static void ReadNgrams() throws IOException {
        Configuration conf = new Configuration();
        Job job = new Job(conf,"readNgrams");

        job.setInputFormatClass(SequenceFileInputFormat.class);
    }
}
