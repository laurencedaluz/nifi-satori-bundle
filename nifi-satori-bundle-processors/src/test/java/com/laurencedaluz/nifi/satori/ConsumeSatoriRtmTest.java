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

import com.satori.rtm.SubscriptionMode;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

//TODO: Test Custom Validators / Configs

public class ConsumeSatoriRtmTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(ConsumeSatoriRtm.class);
    }

    @Test
    public void testProcessor() {

        // Generate a test runner to mock a processor in a flow
        TestRunner runner = TestRunners.newTestRunner(new ConsumeSatoriRtm());

        // Define satori connection properties
        String endpoint = "wss://open-data.api.satori.com";
        String appkey = "";
        String role = "";
        String roleSecretKey = "";
        String channel = "big-rss";
        String filter = "";


        // Add properties
        runner.setProperty(ConsumeSatoriRtm.ENDPOINT, endpoint);
        runner.setProperty(ConsumeSatoriRtm.APPKEY, appkey);
        runner.setProperty(ConsumeSatoriRtm.CHANNEL, channel);
        runner.setProperty(ConsumeSatoriRtm.SUBSCRIPTION_MODE, "SIMPLE");

        runner.run();

        /*
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ConsumeSatoriRtm.SUCCESS);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //assertTrue("1 match", results.size() == 1);
        MockFlowFile result = results.get(0);
        String resultValue = new String(runner.getContentAsByteArray(result));
        System.out.print("\nOUTPUT FILE:\n-----------\n");
        System.out.print(resultValue);
        */

        //TODO: unit testing..
    }

}
