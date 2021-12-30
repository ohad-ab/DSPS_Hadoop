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

class currUser {
    public String bucketName;
    public String jarPath;

    public currUser(String name){
        if (name.equals("Ori")) {
            this.bucketName = "oo-dspsp-ass2";
            this.jarPath = "s3://oo-dspsp-ass2/WordCount.jar";
        }
        else{
            this.bucketName = "dsps-221";
            this.jarPath = "s3://dsps-221/WordCount.jar";
        }
    }
}

public class Main {
    static currUser user = new currUser("Ori");
    static String bucketName = user.bucketName;
    static String jarPath = user.jarPath;

    public static AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();


    //        Link to Google hebrew 3-Grams in S3
    static String nGramsPath = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";

    public static void main(String[] args){
        FromTask(jarPath);
    }

    public static void FromTask(String jarPath){
//        AWSCredentials credentials = new AWSCredentials;
        EmrClient mapReduce = EmrClient.builder().credentialsProvider(credentialsProvider).region(Region.US_EAST_1).build();
        HadoopJarStepConfig hadoopJarStep = HadoopJarStepConfig.builder()
                .jar(jarPath)
                .mainClass("WordCount")
                .args("s3n://"+bucketName+"/input/", "s3n://"+bucketName+"/output/")
                .build();
        StepConfig stepConfig = StepConfig.builder()
                .name("step_wordCount")
                .hadoopJarStep(hadoopJarStep)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
        JobFlowInstancesConfig instances = JobFlowInstancesConfig.builder()
                .instanceCount(2)

                .masterInstanceType("m4.large") //TODO: check what is the free one
                .slaveInstanceType("m4.large")
                .hadoopVersion("2.6.0")//.ec2KeyName("key1")
                .keepJobFlowAliveWhenNoSteps(false)
                .build();
//                .placement(PlacementType.builder().region(Region.US_EAST_1).build()); //TODO: check if needed
        RunJobFlowRequest runFlowRequest = RunJobFlowRequest.builder()
                .name("jobname2")
                .releaseLabel("emr-5.34.0")
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
