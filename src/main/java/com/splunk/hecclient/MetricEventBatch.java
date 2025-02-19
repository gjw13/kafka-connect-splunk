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

import org.apache.commons.lang3.builder.HashCodeBuilder;

public final class MetricEventBatch extends EventBatch {
    public static final String endpoint = "/services/collector";
    public static final String contentType = "application/json; charset=utf-8";

    @Override
    public void add(Event event) {
        if (event instanceof MetricEvent) {
            events.add(event);
            len += event.length();
        } else {
            throw new HecException("only MetricEvent can be add to MetricEventBatch");
        }
    }

    @Override
    public final String getRestEndpoint() {
        return endpoint;
    }

    @Override
    public String getContentType() {
        return contentType;
    }

    @Override
    public EventBatch createFromThis() {
        return new MetricEventBatch();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
        .append(endpoint)
        .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MetricEventBatch) {
            final MetricEventBatch other = (MetricEventBatch) obj;
            return endpoint.equals(other.getRestEndpoint());
        }
        return false;
    }
}
