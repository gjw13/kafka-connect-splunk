/*
 * Copyright 2017 Splunk, Inc..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.splunk.hecclient;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Assert;
import org.junit.Test;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class MetricEventTest {
    static final ObjectMapper jsonMapper = new ObjectMapper();

    final Schema schemaTest = SchemaBuilder.struct()
                .name("test")
                .field("time", Schema.INT64_SCHEMA)
                .field("metric_name:test", Schema.FLOAT64_SCHEMA)
                .field("env", Schema.STRING_SCHEMA)
                .build();

    final Struct struct = new Struct(schemaTest)
                .put("time", 15000000L)
                .put("metric_name:test", 0.0)
                .put("env", "dev");

    @Test
    public void createValidMetricEventWithStruct(){
        SinkRecord rec = new SinkRecord("test_topic", 1, null, "test", null, struct, 0, 0L, TimestampType.NO_TIMESTAMP_TYPE);
        MetricEvent event = new MetricEvent(rec.value(), rec);
        event.setIndex("test-index");
        event.setSourcetype("test-sourcetype");
        event.setSource("test-source");
        event.extractTimestamp();
        Assert.assertEquals(struct, event.getFields());
    }

    @Test
    public void createValidMetricEvent() {
        String data = "this is splunk event";

        // without tied object
        Event event = new MetricEvent(data, null);
        Assert.assertEquals(data, event.getFields());
        Assert.assertEquals(null, event.getTied());

        // with tied object
        String tied = "i love you";
        event = new MetricEvent(data, tied);

        Assert.assertEquals(tied, event.getTied());
        Assert.assertEquals(data, event.getFields());
    }

    @Test(expected = HecException.class)
    public void createInvalidMetricEventWithNullData() {
        Event event = new MetricEvent(null, null);
    }

    @Test(expected = HecException.class)
    public void createInvalidMetricEventWithEmptyString() {
        Event event = new MetricEvent("", null);
    }

    @Test
    public void toStr() {
        SerialAndDeserial sad = new SerialAndDeserial() {
            @Override
            public Event serializeAndDeserialize(Event event) {
                String stringed = event.toString();
                Assert.assertNotNull(stringed);

                Event deserilized;
                try {
                    deserilized = jsonMapper.readValue(stringed, MetricEvent.class);
                } catch (IOException ex) {
                    Assert.assertFalse("expect no exception but got exception", true);
                    throw new HecException("failed to parse MetricEvent", ex);
                }
                return deserilized;
            }
        };
        serialize(sad);
    }

    @Test
    public void getBytes() {
        SerialAndDeserial sad = new SerialAndDeserial() {
            @Override
            public Event serializeAndDeserialize(Event event) {
                byte[] bytes = event.getBytes();
                Assert.assertNotNull(bytes);

                Event deserilized;
                try {
                    deserilized = jsonMapper.readValue(bytes, MetricEvent.class);
                } catch (IOException ex) {
                    Assert.assertFalse("expect no exception but got exception", false);
                    throw new HecException("failed to parse MetricEvent", ex);
                }
                return deserilized;
            }
        };
        serialize(sad);
    }

    @Test
    public void getInputStream() {
        Event event = new MetricEvent("hello", "world");
        InputStream stream = event.getInputStream();
        byte[] data = new byte[1024];
        int siz = UnitUtil.read(stream, data);

        Event eventGot;
        try {
            eventGot = jsonMapper.readValue(data, 0, siz, MetricEvent.class);
        } catch (IOException ex) {
            Assert.assertTrue("failed to deserialize from bytes", false);
            throw new HecException("failed to deserialize from bytes", ex);
        }
        Assert.assertEquals("hello", eventGot.getFields());
    }

    @Test
    public void getterSetter() {
        Event event = new MetricEvent("hello", "world");
        Assert.assertEquals("hello", event.getFields());
        Assert.assertEquals("world", event.getTied());

        Assert.assertNull(event.getIndex());
        event.setIndex("main");
        Assert.assertEquals("main", event.getIndex());

        Assert.assertNull(event.getSource());
        event.setSource("source");
        Assert.assertEquals("source", event.getSource());

        Assert.assertNull(event.getSourcetype());
        event.setSourcetype("sourcetype");
        Assert.assertEquals("sourcetype", event.getSourcetype());

        Assert.assertNull(event.getHost());
        event.setHost("localhost");
        Assert.assertEquals("localhost", event.getHost());

        Assert.assertNull(event.getTime());
        event.setTime(1.0);
        Assert.assertEquals(new Double(1.0), event.getTime());

        event.setTied("hao");
        Assert.assertEquals("hao", event.getTied());
    }

    private interface SerialAndDeserial {
        Event serializeAndDeserialize(final Event event);
    }

    private void serialize(SerialAndDeserial sad) {
        List<Object> eventDataObjs = new ArrayList<>();
        // String object
        eventDataObjs.add("this is splunk event");

        Map<String, String> m = new HashMap<>();
        m.put("hello", "world");

        // Json object
        eventDataObjs.add(m);

        for (Object eventData: eventDataObjs) {
            doSerialize(eventData, sad);
        }
    }

    private void doSerialize(Object data, SerialAndDeserial sad) {
        String tied = "tied";
        Event event = new MetricEvent(data, tied);

        Map<String, String> fields = new HashMap<>();
        fields.put("ni", "hao");
        event.setHost("localhost");
        event.setIndex("main");
        event.setSource("test-source");
        event.setSourcetype("test-sourcetype");
        event.setTime(100000000.0);

        for (int i = 0; i < 2; i++) {
            Event deserialized = sad.serializeAndDeserialize(event);

            Assert.assertEquals(data, deserialized.getFields());
            Assert.assertNull(deserialized.getTied()); // we ignore tied when serialize Event
            Assert.assertEquals("localhost", deserialized.getHost());
            Assert.assertEquals("main", deserialized.getIndex());
            Assert.assertEquals("test-source", deserialized.getSource());
            Assert.assertEquals("test-sourcetype", deserialized.getSourcetype());
            Assert.assertEquals(event.getTime(), deserialized.getTime());
        }
    }
}
