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

public class ConsumeSatoriRtmTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(ConsumeSatoriRtm.class);
    }

    @Test
    public void testConfigValidators() {

        // Generate a test runner to mock a processor in a flow
        TestRunner runner = TestRunners.newTestRunner(new ConsumeSatoriRtm());

        // Define satori connection properties
        runner.setProperty(ConsumeSatoriRtm.ENDPOINT, "wss://open-data.api.satori.com");
        runner.setProperty(ConsumeSatoriRtm.APPKEY, "aaa");
        runner.setProperty(ConsumeSatoriRtm.CHANNEL, "big-rss");
        runner.setProperty(ConsumeSatoriRtm.SUBSCRIPTION_MODE, "SIMPLE");
        runner.setProperty(ConsumeSatoriRtm.BATCH_SIZE, "100");
        runner.assertValid();

        // Test endpoint is not empty
        runner.setProperty(ConsumeSatoriRtm.ENDPOINT, "");
        runner.assertNotValid();
        runner.setProperty(ConsumeSatoriRtm.ENDPOINT, "wss://open-data.api.satori.com");

        // Test Batch size is a valid int
        runner.setProperty(ConsumeSatoriRtm.BATCH_SIZE, "-1");
        runner.assertNotValid();
        runner.setProperty(ConsumeSatoriRtm.BATCH_SIZE, "100");
    }

}
