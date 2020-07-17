package com.demo.aws.kinesis.producer;

import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.regions.*;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.ResourceNotFoundException;
import com.amazonaws.services.kinesisfirehose.*;
import com.amazonaws.services.kinesisfirehose.model.*;
import org.apache.commons.logging.*;
import org.json.simple.*;
import org.json.simple.parser.*;

import java.io.*;
import java.nio.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

public class SmartHomeFirehoseProducer {

    private static final Log LOG = LogFactory.getLog(SmartHomeKinesisProducer.class);

    /**
     * checking for empty argument
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {

        String streamName = "SmartHomeFirehoseStream";
        String regionName = "ap-south-1";
        Region region = RegionUtils.getRegion(regionName);
        if (region == null) {
            System.err.println(regionName + " is not a valid AWS region.");
            System.exit(1);
        }
        final String accessKey = "AKIA5RLBY4LK7WTX5JWV";
        final String secretKey = "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";
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);

        AmazonKinesisFirehose kinesis = AmazonKinesisFirehoseClient.builder()
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
     * @param firehose
     * @param streamName
     */
    private static void sendData(String data, String key, AmazonKinesisFirehose firehose, String streamName) {

        PutRecordRequest putRecord = new PutRecordRequest();
        putRecord.setDeliveryStreamName(streamName);
        Record record = new Record();
        record.setData(ByteBuffer.wrap(data.getBytes()));
        putRecord.setRecord(record);
        try {
            firehose.putRecord(putRecord);
        } catch (AmazonClientException ex) {
            ex.printStackTrace();
            LOG.warn("Error sending record to Amazon Kinesis Firehose.", ex);
        }
    }

    /**
     * Checking whether the stream is already exist in account
     *
     * @param firehose
     * @param streamName
     */
    private static void CheckForStreamAvailability(AmazonKinesisFirehose firehose, String streamName) {
        try {
            DescribeDeliveryStreamRequest req = new DescribeDeliveryStreamRequest();
            req.setDeliveryStreamName(streamName);
            DescribeDeliveryStreamResult result = firehose.describeDeliveryStream(req);
            if (!"ACTIVE".equals(result.getDeliveryStreamDescription().getDeliveryStreamStatus())) {
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
