package com.demo.aws.kinesis.producer;

import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.regions.*;
import com.amazonaws.services.kinesis.*;
import com.amazonaws.services.kinesis.model.*;
import org.apache.commons.logging.*;
import org.json.simple.*;
import org.json.simple.parser.*;

import java.io.*;
import java.nio.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

public class SmartHomeKinesisProducer {


    private static final Log LOG = LogFactory.getLog(SmartHomeKinesisProducer.class);

    /**
     * checking for empty argument
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {

        String streamName = "SmartHomeDataStream";
        String regionName = "ap-south-1";
        Region region = RegionUtils.getRegion(regionName);
        if (region == null) {
            System.err.println(regionName + " is not a valid AWS region.");
            System.exit(1);
        }
        final String accessKey = "AKIA5RLBY4LK7WTX5JWV";
        final String secretKey = "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";
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);

        AmazonKinesis kinesis = AmazonKinesisClient.builder()
                .withRegion(regionName)
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .build();

        CheckForStreamAvailability(kinesis, streamName);

        List<String> files = getFiles("/Users/shakilakhtar/Desktop/aws-assignment/test-data");

        for (int i = 0; i < files.size(); i++) {
            try {

                Scanner scanner = new Scanner(new File(files.get(i)));
                List<JSONObject> jsonArray = new ArrayList<JSONObject>();
                while (scanner.hasNext()) {
                    JSONObject obj = (JSONObject) new JSONParser().parse(scanner.nextLine());
                    jsonArray.add(obj);
                }

                jsonArray.forEach(line -> {
//                    ObjectMapper mapper = new ObjectMapper();
//                    SmartHomeData dataObj = null;
//                    try {
//                        dataObj = mapper.readValue(line.toJSONString(), SmartHomeData.class);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
                    sendData(line.toString(), "partionkey-1", kinesis, streamName);

                });
            } catch (IOException e) {
                System.out.println("Error is reading file at : " + i);
                e.printStackTrace();
            }
        }
        System.out.println("Record processing done!");
    }


    public static List<String> getFiles(String location) {
        List<String> result = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(Paths.get(location))) {

            result = walk.filter(Files::isRegularFile)
                    .map(x -> x.toString()).collect(Collectors.toList());


        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }

    /**
     * @param data       data bytes
     * @param kinesis
     * @param streamName
     */
    private static void sendData(String data, String key, AmazonKinesis kinesis, String streamName) {

        PutRecordRequest putRecord = new PutRecordRequest();
        putRecord.setStreamName(streamName);
        putRecord.setPartitionKey(key);
        putRecord.setData(ByteBuffer.wrap(data.getBytes()));

        try {
            kinesis.putRecord(putRecord);
        } catch (AmazonClientException ex) {
            ex.printStackTrace();
            LOG.warn("Error sending record to Amazon Kinesis.", ex);
        }
    }

    /**
     * Checking whether the stream is already exist in account
     *
     * @param amazonKinesis
     * @param streamName
     */
    private static void CheckForStreamAvailability(AmazonKinesis amazonKinesis, String streamName) {
        try {
            DescribeStreamResult result = amazonKinesis.describeStream(streamName);
            if (!"ACTIVE".equals(result.getStreamDescription().getStreamStatus())) {
                System.err.println("Stream " + streamName + " is not active. Please wait a few moments and try again.");
                System.exit(1);
            }
        } catch (ResourceNotFoundException e) {
            System.err.println("Stream " + streamName + " does not exist. Please create it in the console.");
            System.err.println(e);
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Error found while describing the stream " + streamName);
            System.err.println(e);
            System.exit(1);
        }
    }

}
