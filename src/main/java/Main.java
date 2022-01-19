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
    public String step1JarPath;
    public String step2JarPath;
    public String step3JarPath;
    public String step4JarPath;

    public currUser(String name){
        if (name.equals("Ori")) {
            this.bucketName = "oo-dspsp-ass2";
        }
        else {
            this.bucketName = "dsps-221";
        }
            this.step1JarPath = "s3://"+ bucketName +"/Step1.jar";
            this.step2JarPath = "s3://"+ bucketName +"/Step2.jar";
            this.step3JarPath = "s3://"+ bucketName +"/Step3.jar";
            this.step4JarPath = "s3://"+ bucketName +"/Step4.jar";
    }
}

public class Main {
    static currUser user = new currUser("Ori");
    static String bucketName = user.bucketName;
    static String step1JarPath = user.step1JarPath;
    static String step2JarPath = user.step2JarPath;
    static String step3JarPath = user.step3JarPath;
    static String step4JarPath = user.step4JarPath;

    public static AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();


    //        Link to Google hebrew 3-Grams in S3
    static String nGramsPath = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";

    public static void main(String[] args) {

//        AWSCredentials credentials = new AWSCredentials;
        EmrClient mapReduce = EmrClient.builder().credentialsProvider(credentialsProvider).region(Region.US_EAST_1).build();

//        Step1
        HadoopJarStepConfig hadoopJarStep1 = HadoopJarStepConfig.builder()
                .jar(step1JarPath)
                .mainClass("Step1")
                .args(nGramsPath, "s3n://" + bucketName + "/output1/")
                .build();
        StepConfig stepConfig1 = StepConfig.builder()
                .name("step1")
                .hadoopJarStep(hadoopJarStep1)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

//        Step2
        HadoopJarStepConfig hadoopJarStep2 = HadoopJarStepConfig.builder()
                .jar(step2JarPath)
                .mainClass("Step2")
                .args("s3n://" + bucketName + "/output1/", "s3n://" + bucketName + "/output2/")
                .build();
        StepConfig stepConfig2 = StepConfig.builder()
                .name("step2")
                .hadoopJarStep(hadoopJarStep2)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

//        Step3
        HadoopJarStepConfig hadoopJarStep3 = HadoopJarStepConfig.builder()
                .jar(step3JarPath)
                .mainClass("Step3")
                .args("s3n://" + bucketName + "/output2/", "s3n://" + bucketName + "/output3/")
                .build();
        StepConfig stepConfig3 = StepConfig.builder()
                .name("step3")
                .hadoopJarStep(hadoopJarStep3)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

//        Step4
        HadoopJarStepConfig hadoopJarStep4 = HadoopJarStepConfig.builder()
                .jar(step4JarPath)
                .mainClass("Step4")
                .args("s3n://" + bucketName + "/output3/", "s3n://" + bucketName + "/output4/")
                .build();
        StepConfig stepConfig4 = StepConfig.builder()
                .name("step4")
                .hadoopJarStep(hadoopJarStep4)
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
                .name("3gram statistics")
                .releaseLabel("emr-5.34.0")
                .instances(instances)
                .steps(stepConfig1, stepConfig2, stepConfig3, stepConfig4)
                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("EMR_EC2_DefaultRole")
                .logUri("s3n://" + bucketName + "/logs/")
                .build();
        RunJobFlowResponse runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.jobFlowId();
        System.out.println("Run job flow with id: " + jobFlowId);
    }
}
