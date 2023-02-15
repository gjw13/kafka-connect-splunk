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

import org.apache.http.HttpEntity;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class MetricEventBatchTest {
    @Test
    public void add() {
        Event event = new MetricEvent("ni", "hao");
        EventBatch batch = new MetricEventBatch();
        batch.add(event);
        List<Event> events = batch.getEvents();
        Assert.assertEquals(events.size(), 1);
        Event eventGot = events.get(0);
        Assert.assertEquals(event.getMetricFields(), eventGot.getMetricFields());
        Assert.assertEquals(event.getTied(), eventGot.getTied());
    }

    @Test(expected = HecException.class)
    public void addWithFailure() {
        Event event = new RawEvent("ni", "hao");
        EventBatch batch = new MetricEventBatch();
        batch.add(event);
    }

    @Test
    public void getRestEndpoint() {
        EventBatch batch = new MetricEventBatch();
        Assert.assertEquals(batch.getRestEndpoint(), MetricEventBatch.endpoint);
    }

    @Test
    public void getContentType() {
        EventBatch batch = new MetricEventBatch();
        Assert.assertEquals(batch.getContentType(), MetricEventBatch.contentType);
    }

    @Test
    public void createFromThis() {
        EventBatch batch = new MetricEventBatch();
        EventBatch jsonBatch = batch.createFromThis();
        Assert.assertNotNull(jsonBatch);
        Assert.assertTrue(jsonBatch instanceof MetricEventBatch);
    }

    @Test
    public void isTimedout() {
        EventBatch batch = new MetricEventBatch();
        batch.resetSendTimestamp();
        Assert.assertFalse(batch.isTimedout(1));
        UnitUtil.milliSleep(1000);
        Assert.assertTrue(batch.isTimedout(1));

        // reset timestamp
        batch.resetSendTimestamp();
        Assert.assertFalse(batch.isTimedout(1));
    }

    @Test
    public void setterGetter() {
        EventBatch batch = new MetricEventBatch();
        Assert.assertTrue(batch.isEmpty());
        Assert.assertEquals(batch.length(), 0);
        Assert.assertEquals(batch.size(), 0);
        Assert.assertTrue(batch.isEmpty());
        Assert.assertFalse(batch.isCommitted());
        Assert.assertEquals(batch.getFailureCount(), 0);

        batch.init();
        Assert.assertFalse(batch.isFailed());
        Assert.assertFalse(batch.isCommitted());

        batch.fail();
        Assert.assertTrue(batch.isFailed());
        Assert.assertEquals(batch.getFailureCount(), 1);

        batch.commit();
        Assert.assertTrue(batch.isCommitted());

        Event event = new MetricEvent("ni", "hao");
        System.out.println(event.toString());
        batch.add(event);
        String data = "{\"event\":\"metric\",\"metricFields\":\"ni\"}";
        Assert.assertEquals(data.length() + 1, batch.length());
        Assert.assertEquals(1, batch.size());
        Assert.assertFalse(batch.isEmpty());

        List<Event> events = batch.getEvents();
        Assert.assertEquals(1, events.size());
    }

    @Test
    public void toStr() {
        EventBatch batch = new MetricEventBatch();
        String str = batch.toString();
        Assert.assertEquals("[]", str);

        Event event = new MetricEvent("ni", "hao");
        batch.add(event);
        str = batch.toString();
        Assert.assertEquals(str, "[{\"event\":\"metric\",\"fields\":\"ni\"},]");
    }

    @Test
    public void getHttpEntity() {
        EventBatch batch = new MetricEventBatch();
        HttpEntity entity = batch.getHttpEntity();
        Assert.assertTrue(entity.isRepeatable());
        Assert.assertFalse(entity.isStreaming());
        Assert.assertEquals(0, entity.getContentLength());

        byte[] data = new byte[1024];
        int siz = readContent(entity, data);
        Assert.assertEquals(0, siz);

        Event event = new MetricEvent("ni", "hao");
        batch.add(event);

        entity = batch.getHttpEntity();
        Assert.assertEquals(event.length(), entity.getContentLength());

        siz = readContent(entity, data);
        System.out.println(siz);
        // TODO: figure this out
        String expected = "{\"event\":\"metric\",\"metricFields\":\"ni\"}\n";
        Assert.assertEquals(expected, new String(data, 0, siz));

        // Write to a OutputStream
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            entity.writeTo(out);
        } catch (IOException ex) {
            Assert.assertTrue("failed to write to stream", false);
            throw new HecException("failed to write to stream", ex);
        }
        String got = out.toString();
        Assert.assertEquals(expected, got);
    }

    private int readContent(final HttpEntity entity, byte[] data) {
        // Read from InputStream
        InputStream in;
        try {
            in = entity.getContent();
        } catch (IOException ex) {
            Assert.assertTrue("failed to getContent", false);
            throw new HecException("failed to getContent", ex);
        }

        return UnitUtil.read(in, data);
    }

    @Test
    public void testGZIPCompressionForMetricEvent() {
        EventBatch batch = new MetricEventBatch();
        batch.setEnableCompression(true);
        Assert.assertTrue(batch.isEnableCompression());
        Event event = new MetricEvent("hello world! hello world! hello world!", "hao");
        batch.add(event);
        HttpEntity entity = batch.getHttpEntityTemplate();
        byte[] data = new byte[1024];
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            entity.writeTo(out);
            String expected = "{\"event\":\"metric\",\"metricFields\":\"hello world! hello world! hello world!\"}\n";
            ByteArrayInputStream bis = new ByteArrayInputStream(out.toByteArray());
            GZIPInputStream gis = new GZIPInputStream(bis);
            int read = gis.read(data, 0, data.length);
            gis.close();
            bis.close();

            // Decode the bytes into a String
            String ori = new String(data, 0, read, "UTF-8");
            Assert.assertEquals(expected, ori);
        } catch (IOException ex) {
            Assert.assertTrue("failed to compress and decompress the data", false);
            throw new HecException("failed to compress and decompress the data", ex);
        }
    }
}
