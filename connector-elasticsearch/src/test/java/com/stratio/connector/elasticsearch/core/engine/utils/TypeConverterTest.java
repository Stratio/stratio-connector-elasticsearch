/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
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

package com.stratio.connector.elasticsearch.core.engine.utils;

import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * TypeConverter Tester.
 *
 * @author <Authors name>
 * @version 1.0
 */
public class TypeConverterTest {

    /**
     * Method: convert(ColumnType columnType)
     */

    private Map<ColumnType, String> convet = new HashMap<>();

    private List<ColumnType> exceptions = new LinkedList<>();

    @Before
    public void setUp() {
        convet.put(new ColumnType(DataType.BOOLEAN), "boolean");
        convet.put(new ColumnType(DataType.BIGINT), "long");
        convet.put(new ColumnType(DataType.DOUBLE), "double");
        convet.put(new ColumnType(DataType.FLOAT), "float");
        convet.put(new ColumnType(DataType.INT), "integer");
        convet.put(new ColumnType(DataType.TEXT), "string");
        convet.put(new ColumnType(DataType.VARCHAR), "string");
        ColumnType listType = new ColumnType(DataType.LIST, null);
        listType.setDBCollectionType(new ColumnType(DataType.TEXT));
        convet.put(listType, "string");

        exceptions.add(new ColumnType(DataType.MAP));
        exceptions.add(new ColumnType(DataType.NATIVE));
        exceptions.add(new ColumnType(DataType.SET));

    }

    @Test
    public void testConvert() throws Exception {

        allCoveredTest();
        conversionTest();
        exceptionTest();

    }

    private void allCoveredTest() {
        assertEquals("All features are tested", DataType.values().length, convet.size() + exceptions.size());
    }

    private void conversionTest() throws UnsupportedException, ExecutionException {
        for (Map.Entry<ColumnType, String> convertedTypes : convet.entrySet()) {
            assertEquals("The conversion is correct", convertedTypes.getValue(),
                            TypeConverter.convert(convertedTypes.getKey()));
        }
    }

    private void exceptionTest() {
        for (ColumnType columnType : exceptions) {
            try {
                TypeConverter.convert(columnType);
            } catch (ExecutionException e) {
                continue;
            }
            fail("The execution musn't  are here with this column type " + columnType);
        }
    }

}
