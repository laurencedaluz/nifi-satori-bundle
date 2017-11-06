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

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import com.satori.rtm.*;
import com.satori.rtm.auth.*;

//TODO: Add support for HTTPS proxy

@Tags({"pubsub", "satori", "rtm", "realtime", "json"})
@CapabilityDescription("Publishes incoming messages to Satori RTM (https://www.satori.com/docs/using-satori/overview)")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SupportsBatching
public class PublishSatoriRTM extends AbstractProcessor {

    public static final PropertyDescriptor ENDPOINT = new PropertyDescriptor
            .Builder().name("ENDPOINT")
            .displayName("Endpoint")
            .description("Entry point to RTM")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("wss://open-data.api.satori.com")
            .build();

    public static final PropertyDescriptor APPKEY = new PropertyDescriptor
            .Builder().name("APPKEY")
            .displayName("Appkey")
            .description("String used by RTM to identify the client when it connects to RTM")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor ROLE = new PropertyDescriptor
            .Builder().name("ROLE")
            .displayName("Role")
            .description("Role and Role Secret Key are only required when connecting to a channel that is explicitly "
                    + "configured with channel permissions requiring authentication. Not required for consuming Open Data Channels.")
            .required(false)
            .build();

    public static final PropertyDescriptor ROLE_SECRET_KEY = new PropertyDescriptor
            .Builder().name("ROLE_SECRET_KEY")
            .displayName("Role Secret Key")
            .description("Role and Role Secret Key are only required when connecting to a channel that is explicitly "
                    + "configured with channel permissions requiring authentication. Not required for consuming Open Data Channels.")
            .required(false)
            .addValidator(Validator.VALID)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor CHANNEL = new PropertyDescriptor
            .Builder().name("CHANNEL")
            .displayName("Channel")
            .description("Name of channel to publish to")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

  /*  public static final PropertyDescriptor MSG_DEMARCATOR = new PropertyDescriptor
            .Builder().name("MSG_DEMARCATOR")
            .displayName("Message Demarcator")
            .description("//todo")
            .addValidator(Validator.VALID)
            .required(false)
            .build();*/

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles published successfully to RTM.")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles failed to publish to RTM.")
            .build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;


    private volatile RtmClient client;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(ENDPOINT);
        descriptors.add(APPKEY);
        descriptors.add(ROLE);
        descriptors.add(ROLE_SECRET_KEY);
        descriptors.add(CHANNEL);
        //descriptors.add(MSG_DEMARCATOR);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

        // Get the user defined configuration properties
        String endpoint = context.getProperty(ENDPOINT).getValue();
        String appkey = context.getProperty(APPKEY).getValue();
        String role = context.getProperty(ROLE).getValue();
        String roleSecretKey = context.getProperty(ROLE_SECRET_KEY).getValue();
        boolean shouldAuthenticate = context.getProperty(ROLE).isSet();

        // Connect to satori
        final RtmClientBuilder builder = new RtmClientBuilder(endpoint, appkey)
                .setListener(new RtmClientAdapter() {
                    @Override
                    public void onConnectingError(RtmClient client, Exception ex) {
                        String msg = String.format("RTM client failed to connect to '%s': %s",
                                endpoint, ex.getMessage());
                        getLogger().error(msg);
                    }

                    @Override
                    public void onError(RtmClient client, Exception ex) {
                        String msg = String.format("RTM client failed: %s", ex.getMessage());
                        getLogger().error(msg);
                    }

                    @Override
                    public void onEnterConnected(RtmClient client) {
                        getLogger().info("Connected to Satori!");
                    }

                });

        // Authenticate with role and secret key if required
        if (shouldAuthenticate) {
            builder.setAuthProvider(new RoleSecretAuthProvider(role, roleSecretKey));
        }

        client = builder.build();

        getLogger().info(String.format(
                "RTM connection config:\n" +
                        "\tendpoint='%s'\n" +
                        "\tappkey='%s'\n" +
                        "\tauthenticate?=%b", endpoint, appkey, shouldAuthenticate));

        client.start();

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        // Get required user properties
        String channel = context.getProperty(CHANNEL).getValue();
        // TODO: Implement Message Demarcator
        //String msgDemarcator = context.getProperty(MSG_DEMARCATOR).getValue();
        //boolean useDemarcator = context.getProperty(MSG_DEMARCATOR).isSet();

        // Read incoming message string
        final AtomicReference<String> contentsRef = new AtomicReference<>(null);

        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                final String contents = IOUtils.toString(in, "UTF-8");
                contentsRef.set(contents);
            }
        });

        final String message = contentsRef.get();

        // Publish message to Satori
        try {
            // Publish message to Satori
            client.publish(channel, message, Ack.YES);//TODO: Ack config property

            // Success!
            session.transfer(flowFile, SUCCESS);
        } catch (Throwable t)
        {
            // Failure!
            getLogger().error("Unable to process file: \n" + t.getMessage());
            session.transfer(flowFile, FAILURE);
            context.yield();
        }

    }

    @OnStopped
    public void shutdownClient() {
        if (client != null) {
            client.stop();
        }
    }
}
