/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.contrib.connect;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import org.junit.Test;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletionException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaConnectSimulatorIntegrationTest extends JetTestSupport {


    @Test
    public void readFromSimulatorMqViaConnect() {
        Properties rabbitMqProperties = new Properties();
        rabbitMqProperties.setProperty("name", "simulator-source-connector");
        rabbitMqProperties.setProperty("connector.class",
                "com.github.jcustenborder.kafka.connect.simulator.SimulatorSourceConnector");
        rabbitMqProperties.setProperty("topic", "messages");
        rabbitMqProperties.setProperty("key.schema.fields", "nationalIdentificationNumber");
        rabbitMqProperties.setProperty("value.schema.fields", "firstName,lastName");

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(KafkaConnectSources.connect(rabbitMqProperties))
                .withoutTimestamps()
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> assertGreaterOrEquals("less than 100 events", list.size(), 100)));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(Objects.requireNonNull(this.getClass()
                                                          .getClassLoader()
                                                          .getResource("kafka-connect-simulator-0.1.118.zip"))
                                      .getPath());


        Job job = createJetMember().newJob(pipeline, jobConfig);
        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
        }

    }
}
