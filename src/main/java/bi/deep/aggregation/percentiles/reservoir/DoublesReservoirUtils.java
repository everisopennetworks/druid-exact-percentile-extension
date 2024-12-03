/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package bi.deep.aggregation.percentiles.reservoir;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DoublesReservoirUtils {
    public static final ObjectMapper MAPPER = new ObjectMapper();

    private DoublesReservoirUtils() {
        throw new AssertionError("No DoublesReservoirUtils instances for you!");
    }

    public static String convertToJson(DoublesReservoir reservoir) throws JsonProcessingException {
        return MAPPER.writeValueAsString(reservoir);
    }

    public static DoublesReservoir readJson(String value) throws JsonProcessingException {
        return MAPPER.readValue(value, DoublesReservoir.class);
    }

    public static DoublesReservoir convert(Object content) {
        return MAPPER.convertValue(content, DoublesReservoir.class);
    }
}
