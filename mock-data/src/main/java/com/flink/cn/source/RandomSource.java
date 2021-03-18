package com.flink.cn.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RandomSource extends RichSourceFunction<String> {

    private transient ObjectMapper objectMapper;
    private volatile String recordJson;
    // it is important
    private volatile Boolean bRunning = true;

    private int expectedRate;

    private List<String> deviceIds = new ArrayList<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        objectMapper = new ObjectMapper();
        recordJson = readFile();

        ParameterTool globalJobParameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        int deviceNum = globalJobParameters.getInt("deviceNum", 10);
        mockDeviceIds(deviceNum);

        expectedRate = globalJobParameters.getInt("rate", 1);


        log.info("\nRecord: {}", objectMapper.writeValueAsString(recordJson));
        log.info("\nRate is: {}", expectedRate);
        log.info("\nDeviceIds are: {}", Arrays.toString(deviceIds.toArray()));
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        long startTimeStamp, nowTimeStamp;
        while (bRunning) {
            startTimeStamp = System.currentTimeMillis();

            Map<String, Object> motorMap = objectMapper.readValue(recordJson, Map.class);
            Map motorVehicleListObject = (Map) motorMap.get("MotorVehicleListObject");
            List motorVehicleObject = (List) motorVehicleListObject.get("MotorVehicleObject");
            for (Object motor : motorVehicleObject) {
                Map map = (Map) motor;
                map.put("DeviceID", deviceIds.get(new Random().nextInt(deviceIds.size())));
            }
            motorMap.put("AppearTime", System.currentTimeMillis());

            nowTimeStamp = System.currentTimeMillis();

            if (nowTimeStamp - startTimeStamp > 1000) {
                expectedRate += expectedRate;
                continue;
            }

            ctx.collect(objectMapper.writeValueAsString(motorMap));

            if (--expectedRate == 0) {
                TimeUnit.SECONDS.sleep(1);
            } else {
                TimeUnit.MILLISECONDS.sleep(400);
            }


        }
    }


    @Override
    public void cancel() {
        this.bRunning = false;
    }

    private String readFile() throws IOException {
        StringBuffer sb = new StringBuffer();
        InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("motorjson.txt");
        BufferedInputStream bInp = new BufferedInputStream(resourceAsStream);
        byte[] buffer = new byte[1024];
        int bytesRead = 0;
        while ((bytesRead = bInp.read(buffer)) != -1) {
            sb.append(new String(buffer, 0, bytesRead));
        }
        return sb.toString();
    }

    private void mockDeviceIds(int deviceNum) {
        for (int i = 0; i < deviceNum; i++) {
            deviceIds.add("40001" + String.valueOf(i));
        }
    }

}
