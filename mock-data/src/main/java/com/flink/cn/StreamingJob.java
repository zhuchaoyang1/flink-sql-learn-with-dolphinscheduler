/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flink.cn;

import com.flink.cn.source.RandomSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.internal.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
@Slf4j
public class StreamingJob {

    public static Properties propertiesOut = null;

    static {
        try {
            propertiesOut = new Properties();
            propertiesOut.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("kafka.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool tools = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(tools);

        if (checkParameters(tools)) {
            throw new RuntimeException("Some Parameters has not set");
        }

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, propertiesOut.getProperty("kafka_broker_list"));

        FlinkKafkaProducer011 flinkKafkaProducer011 = new FlinkKafkaProducer011(
                propertiesOut.getProperty("push_topic"),
                new SimpleStringSchema(),
                properties);

        DataStreamSource<String> mockStream = env.addSource(new RandomSource());
        mockStream.name("MockSource");  // 只支持并行度为1

        if (tools.getBoolean("sideOut", true)) {
            SingleOutputStreamOperator<String> process = mockStream.process(new ProcessFunction<String, String>() {
                @Override
                public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                    log.info(value);
                }
            });
            process.name("Sideout").disableChaining();
        }

        mockStream.addSink(flinkKafkaProducer011).name("SinkKfk");

        env.execute("MockMotorDatasIntoKfk");
    }

    private static boolean checkParameters(ParameterTool tool) {
        Object sideOut = tool.get("sideOut");
        Object deviceNum = tool.get("deviceNum");
        Object rate = tool.get("rate");
        if (sideOut == null || deviceNum == null || rate == null) {
            return true;
        }
        return false;
    }

}
