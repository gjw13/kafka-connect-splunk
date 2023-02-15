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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.nio.charset.StandardCharsets;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import org.slf4j.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 *  MetricEvent is used as the Object to represented Splunk events when the /services/collector HEC endpoint is to
 *  be used for Splunk ingestion.
 * <p>
 * TODO: rewrite
 * This class contains overridden methods from Event which will allow adding extra fields to events,
 * retrieving extra fields, converting the MetricEvent object to a String and converting the MetricEvent object into a byte
 * representation.
 * @see         Event
 * @version     1.0
 * @since       1.0
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class Metric {
    // private Map<String, String> fields;
    static final String TIME = "time";
    static final String HOST = "host";
    static final String INDEX = "index";
    static final String SOURCE = "source";
    static final String SOURCETYPE = "sourcetype";

    static final ObjectMapper jsonMapper;
    static {
        jsonMapper = new ObjectMapper();
        jsonMapper.registerModule(new com.splunk.kafka.connect.JacksonStructModule());
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        jsonMapper.setDateFormat(df);
    }

    protected static final Logger log = LoggerFactory.getLogger(Metric.class);

    protected String source;
    protected String sourcetype;
    protected String host;
    protected String index;
    protected String event = "metric";

    @JsonSerialize(using = DoubleSerializer.class)
    protected Double time = null; // epoch seconds.milliseconds

    private Object fields;
    @JsonIgnore
    private Object tied;

    @JsonIgnore
    protected String lineBreaker = "\n";

    @JsonIgnore
    protected byte[] bytes;

    /**
     * Creates a new metric event.
     *
     * @param data    Object representation of the event itself without all the extras. Event Data Only
     * @param tied    Object representation of the entire Record being constructed into an Event.
     *
     * @since         1.0
     * @see           Event
     */
    public Metric(Object fields, Object tied) {
        this.fields = fields;
        this.tied = tied;
    }

    /**
     * Creates a new json event with default values.
     *
     * @since           1.0
     */
    Metric() {
    }

    /**
     * Event is the data portion of the Event Record. Data passed in is validated to be an acceptable String and the byte[]
     * representation of the Event is cleared as the Event representation has changed.
     *
     * @param  data  Object representation of the event itself without all the extras. Event Data Only
     * @return       Current representation of Event.
     * @see          Event
     * @since        1.0.0
     */
    public final Metric setFields(final Object data) {
        // checkEventData(data);
        fields = data;
        // invalidate();
        return this;
    }

    /**
     * Time is the Long representation of the event time in epoch format. This is to be later used as the time field in
     * an indexed Splunk Event.
     *
     * @param etime Double representation of the record event in time.seconds.milliseconds
     * @return      Current representation of Event.
     * @see         Event
     * @since       1.0.0
     */
    public final Metric setTime(final double etime /* seconds.milliseconds */) {
        this.time = etime;
        // invalidate();
        return this;
    }

    /**
     * Tied is the full Record Object with associated meta-data.
     *
     * @param tied   Object representation of the event with associated meta-data.
     * @return       Current representation of Event.
     * @see          Event
     * @since        1.0.0
     */
    public final Metric setTied(final Object tied) {
        this.tied = tied;
        return this;
    }

    /**
     * Source is the default field used within an indexed Splunk event. The source of an event is the name of the file, stream
     * or other input from which the event originates
     *
     * @param source String representation of the record event source.
     * @return       Current representation of Event.
     * @see          Event
     * @since        1.0.0
     */
    public final Metric setSource(final String source) {
        this.source = source;
        // invalidate();
        return this;
    }

    /**
     * Sourcetype is the default field used within an indexed Splunk event. The source type of an event is the format
     * of the data input from which it originates.The source type determines how your data is to be formatted.
     *
     * @param sourcetype String representation of the record event sourcetype.
     * @return           Current representation of Event.
     * @see              Event
     * @since            1.0.0
     */
    public final Metric setSourcetype(final String sourcetype) {
        this.sourcetype = sourcetype;
        // invalidate();
        return this;
    }

    /**
     * Host is the default field used within an indexed Splunk event. An event host value is typically the hostname,
     * IP address, or fully qualified domain name of the network host from which the event originated. The host value
     * lets you locate data originating from a specific device.
     *
     * @param host String representation of the host machine which generated the event.
     * @return     Current representation of Event.
     * @see        Event
     * @since      1.0.0
     */
    public final Metric setHost(final String host) {
        this.host = host;
        // invalidate();
        return this;
    }

    /**
     * Index is a required field used to send an event to particular <a href=http://docs.splunk.com/Documentation/Splunk/7.0.0/Indexer/Aboutindexesandindexers>Splunk Index</>.
     *
     * @param index String representation of the Splunk index
     * @return      Current representation of Event.
     * @see         Event
     * @since       1.0.0
     */
    public final Metric setIndex(final String index) {
        this.index = index;
        // invalidate();
        return this;
    }

    /**
     * Index is a required field used to send an event to particular <a href=http://docs.splunk.com/Documentation/Splunk/7.0.0/Indexer/Aboutindexesandindexers>Splunk Index</>.
     *
     * @param index String representation of the Splunk index
     * @return      Current representation of Event.
     * @see         Event
     * @since       1.0.0
     */
    public final Metric setEvent(final String event) {
        this.event = event;
        // invalidate();
        return this;
    }

    public final Double getTime() {
        return time;
    }

    public final String getSource() {
        return source;
    }

    public final String getSourcetype() {
        return sourcetype;
    }

    public final String getHost() {
        return host;
    }

    public final String getIndex() {
        return index;
    }

    public final String getEvent() {
        return event;
    }

    public final Object getTied() {
        return tied;
    }

    /**
     * ExtraFields consist of custom fields used for enriching events to be bundled in with the base Event. This can
     * used to categorize certain events, allowing flexibility of searching for this field after ingested in Splunk.
     *
     * @return             Map representation of fields
     * @see                Map
     * @since              1.0
     */
    // @Override
    public Object getFields() {
        return fields;
    }

    /**
     * Will calculate and return the amount of bytes as an integer of the data and linebreak combined. Used in batch
     * classes to calculate the total length of a batch to fulfil interface requirements of org.apache.http.HttpEntity
     *
     * @return  the total number of bytes of the eventEvent
     * @see     org.apache.http.HttpEntity
     * @since   1.0.0
     */
    public final int length() {
        byte[] data = getBytes();
        return data.length + lineBreaker.getBytes().length;
    }

    /**
     * Using ObjectMapper the MetricEvent is serialized to a String and returned.
     *
     * @return  Serialized String representation of MetricEvent including all variables in superclass Event.
     *
     * @throws  HecException
     * @see     com.fasterxml.jackson.databind.ObjectMapper
     * @since   1.0
     */
    @Override
    public String toString() {
        try {
            return jsonMapper.writeValueAsString(this);
        } catch (Exception ex) {
            System.out.println("failed to json serlized MetricEvent: " + ex.toString());
            throw new HecException("failed to json serialized JsonEvent", ex);
        }
    }

    /**
     * Creates a concatenated InputStream buffered with event data and linebreak data. Linebreak is inserted to avoid
     * "copying" the event.
     *
     * @return  An InputStream which has buffered the Event data, and linebreak data in bytes.
     *
     * @see     java.io.InputStream
     * @see     java.io.SequenceInputStream
     * @since   1.0.0
     */
    @JsonIgnore
    public final InputStream getInputStream() {
        byte[] data = getBytes();
        InputStream eventStream = new ByteArrayInputStream(data);

        // avoid copying the event
        InputStream carriageReturnStream = new ByteArrayInputStream(lineBreaker.getBytes());
        return new SequenceInputStream(eventStream, carriageReturnStream);
    }

    /**
     * Retrieves byte representation of Event's extended classes JsonEvent, RawEvent, MetricEvent and writes bytes to OutputStream
     * provided as a parameter. After the Event is written to stream a linebreak is also written to separate events.
     *
     * @param out OutputStream to write byte representation of Event(JSONEvent, RawEvent, MetricEvent) to.
     *
     * @throws  IOException
     * @see     java.io.OutputStream
     * @since   1.0.0
     */
    public final void writeTo(OutputStream out) throws IOException {
        byte[] data = getBytes();
        out.write(data);

        // append line breaker
        byte[] breaker = lineBreaker.getBytes();
        out.write(breaker);
    }

    /**
     * Checks to ensure the byte representation of the Event has not already been calculated. If so, it will return
     * what is already in variable bytes. Otherwise the ObjectMapper through annotations will serialize the
     * MetricEvent object.
     *
     * @return  Serialized byte array representation of MetricEvent including all variables in superclass Event. Will return the
     * value already contained in bytes if it is not null for the Event.
     *
     * @throws  HecException
     * @see     com.fasterxml.jackson.databind.ObjectMapper
     * @since   1.0
     */
    // @Override
    @JsonIgnore
    public byte[] getBytes() {
        if (bytes != null) {
            return bytes;
        }
        try {
            bytes = jsonMapper.writeValueAsBytes(this);
        } catch (Exception ex) {
            log.error("Invalid json event", ex);
            throw new HecException("Failed to json marshal the event", ex);
        }
        return bytes;
    }

    /**
     * Extracts timestamp from metric fields. Assumes time key is `time`.
     */
    @JsonIgnore
    public void extractTimestamp() {
        String jsonStr = this.getFields().toString();
        String string = jsonStr.replaceAll("\\\"", "\"");
        String timestamp = "";

        String re = "\\\"time\\\":\\s*\\\"(?<time>.*?)\"";
        final Pattern pattern = Pattern.compile(re);
        final Matcher matcher = pattern.matcher(string);
        try {
            if (matcher.find()) {
                timestamp = (matcher.group("time"));
            }
        } catch (Exception e) {
            log.warn("Couldn't extract metric timestamp", e);
        }
        try {
            double epoch;
            epoch = ((Double.parseDouble(timestamp)));
            long long_epoch = (new Double(epoch)).longValue();
            System.out.println(long_epoch);
            this.setTime(epoch / (Math.pow(10, Long.toString(long_epoch).length()-10)));
        } catch (Exception e) {
            log.warn("Could not set the time", e);
        }
    }
}

