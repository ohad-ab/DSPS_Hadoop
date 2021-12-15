import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.emr.EmrClientBuilder;
import software.amazon.awssdk.services.emr.model.*;



public class Main {
    public static DefaultAWSCredentialsProviderChain credentialsProvider = new DefaultAWSCredentialsProviderChain();
    public static void main(String[] args){
//        Link to Google hebrew 3-Grams in S3
//        s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data
//        AWSCredentials credentials = new PropertiesCredentials();
        EmrClientBuilder mapReduce = new
                EmrClientBuilder(new EnvironmentVariableCredentialsProvider());
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3n://yourbucket/yourfile.jar") // This should be a full map reduce application.
.withMainClass("some.pack.MainClass")
                .withArgs("s3n://yourbucket/input/", "s3n://yourbucket/output/");
        StepConfig stepConfig = new StepConfig()
                .withName("stepname")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.6.0").withEc2KeyName("yourkey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("jobname")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withLogUri("s3n://yourbucket/logs/");
        RunJobFlowResponse runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
