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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *  MetricEvent is used as the Object to represented Splunk events when the /services/collector HEC endpoint is to
 *  be used for Splunk ingestion.
 * <p>
 * This class contains overridden methods from Event which will allow adding extra fields to events,
 * retrieving extra fields, converting the MetricEvent object to a String and converting the MetricEvent object into a byte
 * representation.
 * @see         Event
 * @version     1.0
 * @since       1.0
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class MetricEvent extends Event {
    protected Object fields;

    @JsonInclude
    protected String event = "metric"; // https://docs.splunk.com/Documentation/Splunk/9.0.2/Metrics/GetMetricsInOther#Send_data_to_a_metrics_index_over_HTTP

    /**
     * Creates a new metric event.
     *
     * @param data    Object representation of the event itself without all the extras. Event Data Only
     * @param tied    Object representation of the entire Record being constructed into an Event.
     *
     * @since         1.0
     * @see           Event
     */
    public MetricEvent(Object data, Object tied) {
        checkMetricData(data);
        this.setTied(tied);
        this.fields = data;
    }

    /**
     * Creates a new json event with default values.
     *
     * @since           1.0
     */
    MetricEvent() {
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
    public final MetricEvent setFields(final Object data) {
        checkMetricData(data);
        fields = data;
        invalidate();
        return this;
    }

    /**
     * ExtraFields consist of custom fields used for enriching events to be bundled in with the base Event. This can
     * used to categorize certain events, allowing flexibility of searching for this field after ingested in Splunk.
     *
     * @return             Map representation of fields
     * @see                Object
     * @since              1.0
     */
    public Object getFields() {
        // Map<String, String> mapping = null;
        // try {
        //     mapping = new ObjectMapper().readValue(fields.toString(), Map.class);
        // } catch (Exception ex) {
        //     throw new HecException("failed to json serialized JsonEvent", ex);
        // }
        // return mapping;
        return fields;
    }

    public final String getEvent() {
        return event;
    }

    // /**
    //  * ExtraFields consist of custom fields used for enriching events to be bundled in with the base Event. This can
    //  * used to categorize certain events, allowing flexibility of searching for this field after ingested in Splunk.
    //  * This differs from the setFields method as it will append any extra fields to the the
    //  *
    //  * @param extraFields  Object representation of the event with associated meta-data.
    //  * @return             Current representation of MetricEvent.
    //  * @see                MetricEvent
    //  * @since              1.0
    //  */
    // @Override
    // public MetricEvent addFields(final Map<String, String> extraFields) {
    //     if (extraFields == null || extraFields.isEmpty()) {
    //         return this;
    //     }

    //     if (fields == null) {
    //         fields = new HashMap<>();
    //     }

    //     fields.putAll(extraFields);
    //     invalidate();

    //     return this;
    // }

    // /**
    //  * ExtraFields consist of custom fields used for enriching events to be bundled in with the base Event. This can
    //  * used to categorize certain events, allowing flexibility of searching for this field after ingested in Splunk.
    //  * This differs from the addFields method as it will replace any fields that are currently associated to this object.
    //  *
    //  * @param extraFields  Object representation of the event with associated meta-data.
    //  * @return             Current representation of MetricEvent.
    //  * @see                MetricEvent
    //  * @since              1.0
    //  */
    // @Override
    // public MetricEvent setFields(final Map<String, String> extraFields) {
    //     fields = extraFields;
    //     invalidate();
    //     return this;
    // }

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
            //backtrace here
            log.warn("calling toString for MetricEvent");
            // Thread.currentThread().dumpStack();
            // return jsonMapper.writeValueAsString(this).replace("metricFields", "fields");
            return jsonMapper.writeValueAsString(this);
        } catch (Exception ex) {
            System.out.println("failed to json serlized MetricEvent: " + ex.toString());
            throw new HecException("failed to json serialized JsonEvent", ex);
        }
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
    @Override
    public byte[] getBytes() {
        log.info("Calling getBytes for MetricEvent");
        if (bytes != null) {
            return bytes;
        }

        try {
            bytes = jsonMapper.writeValueAsBytes(this);
        } catch (Exception ex) {
            log.error("Invalid metric event", ex);
            throw new HecException("Failed to json marshal the metric event", ex);
        }
        return bytes;
    }

    private static void checkMetricData(Object eventData) {
        if (eventData == null) {
            throw new HecNullEventException("Null data for metric event");
        }
        if (eventData instanceof String) {
            if (((String) eventData).isEmpty()) {
                throw new HecEmptyEventException("Empty metric event");
            }
        }
    }

    /**
     * Extracts timestamp from metric fields. Assumes time key is `time`.
     */
    @JsonIgnore
    public void extractTimestamp() {
        String jsonStr = this.getFields().toString();
        log.info("Fields: " + jsonStr);
        String string = jsonStr.replaceAll("\\\"", "\"");
        String timestamp = "";

        // Because records from Kafka Connect come in as a Struct, regex is set using = operator
        String re = "time=(?<time>.*?),";
        final Pattern pattern = Pattern.compile(re);
        final Matcher matcher = pattern.matcher(string);
        log.info("timestamp matcher: " + matcher.toString());
        try {
            if (matcher.find()) {
                timestamp = (matcher.group("time"));
                log.info("Timestamp: " + timestamp);
            }
        } catch (Exception e) {
            log.warn("Couldn't extract metric timestamp", e);
        }
        try {
            double epoch;
            epoch = ((Double.parseDouble(timestamp)));
            log.info("Epoch: " + epoch);
            long long_epoch = (new Double(epoch)).longValue();
            log.info("Long epoch: " + long_epoch);
            this.setTime(epoch / (Math.pow(10, Long.toString(long_epoch).length()-10)));
        } catch (Exception e) {
            log.warn("Could not set the time", e);
        }
    }
}
