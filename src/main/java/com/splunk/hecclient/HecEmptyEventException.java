/*
 * Copyright 2018 Splunk, Inc..
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

/**
 * HecEmptyEventException is an exception which is triggered during the creation of an Event(JsonEvent, RawEvent, or MetricEvent)
 * when Event is created with an empty String ("").
 *
 * @version     1.1.0
 * @since       1.1.0
 */
public class HecEmptyEventException extends HecException {
    private static final long serialVersionUID = 34L;

    public HecEmptyEventException(String message) {
        super(message);
    }

    public HecEmptyEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
