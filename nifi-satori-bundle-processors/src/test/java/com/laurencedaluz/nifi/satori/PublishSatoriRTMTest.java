/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.laurencedaluz.nifi.satori;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class PublishSatoriRTMTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(PublishSatoriRTM.class);
    }

    @Test
    public void testProcessor() {

        // Content to be mock a geo csv file
        String inputJson = "{\"test1\": 1, \"testOne\": \"one\"}";
        InputStream content = new ByteArrayInputStream(inputJson.getBytes());

        // Generate a test runner to mock a processor in a flow
        TestRunner runner = TestRunners.newTestRunner(new PublishSatoriRTM());

        // Define satori connection properties
        runner.setProperty(PublishSatoriRTM.ENDPOINT, "wss://qwxhvwv6.api.satori.com");
        runner.setProperty(PublishSatoriRTM.APPKEY, "ed444FdFF8Fa73Fc65F6EA2202DfB33E");
        runner.setProperty(PublishSatoriRTM.CHANNEL, "nifitest");
        runner.assertValid();

        // Add the content to the runner
        runner.enqueue(content);
        runner.enqueue(content);
        runner.enqueue(content);
        runner.enqueue(content);
        runner.enqueue(content);

        // Run the enqueued content, it also takes an int = number of contents queued
        runner.run(1);
    }

}
