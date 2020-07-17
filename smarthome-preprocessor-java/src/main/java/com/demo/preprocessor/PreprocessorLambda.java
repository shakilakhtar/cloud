package com.demo.preprocessor;

import com.amazonaws.services.lambda.runtime.*;
import com.amazonaws.services.lambda.runtime.events.*;
import com.demo.preprocessor.domain.*;
import com.demo.preprocessor.service.*;
import com.fasterxml.jackson.databind.*;


import java.nio.*;
import java.util.*;

public class PreprocessorLambda implements
        RequestHandler<KinesisAnalyticsStreamsInputPreprocessingEvent, KinesisAnalyticsInputPreprocessingResponse> {


    @Override
    public KinesisAnalyticsInputPreprocessingResponse handleRequest(
            KinesisAnalyticsStreamsInputPreprocessingEvent event, Context context) {
        context.getLogger().log("InvocatonId is : " + event.invocationId);
        context.getLogger().log("StreamArn is : " + event.streamArn);
        context.getLogger().log("ApplicationArn is : " + event.applicationArn);

        List<KinesisAnalyticsInputPreprocessingResponse.Record> records = new ArrayList<KinesisAnalyticsInputPreprocessingResponse.Record>();
        KinesisAnalyticsInputPreprocessingResponse response = new KinesisAnalyticsInputPreprocessingResponse(records);

        event.records.stream().forEach(record -> {
            context.getLogger().log("recordId is : " + record.recordId);
            context.getLogger().log("record aat is :" + record.kinesisStreamRecordMetadata.approximateArrivalTimestamp);
            //check temperature value to find anomalies
            //Anomaly is defined as +/-5ºF delta between the top floor temperature and the thermostat set temperature
            ByteBuffer bb1 = ByteBuffer.allocate(10);

            // putting the value in ByteBuffer
            bb1.put((byte) 10);
            bb1.put((byte) 20);
            String payload = bb1.toString();
            ObjectMapper mapper = new ObjectMapper();
            try {
                SmartHomeData dataObj = mapper.readValue(payload, SmartHomeData.class);

                KinesisAnalyticsInputPreprocessingResponse.Record r = new KinesisAnalyticsInputPreprocessingResponse.Record();
                ThermostatService ts = new ThermostatServiceImpl();
                float currentTemp = dataObj.getTemperature();
                float desiredTemp = ts.getTemperature();
                //filter record
                if (currentTemp - desiredTemp >= 5 || currentTemp - desiredTemp <= -5) {
                    // Add your record.data pre-processing logic here.
                    response.records.add(new KinesisAnalyticsInputPreprocessingResponse.Record(record.recordId, KinesisAnalyticsInputPreprocessingResponse.Result.Ok, record.getData()));

                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        return response;
    }

}
