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

package com.stratio.connector.elasticsearch.core.engine.metadata;

import com.stratio.crossdata.common.data.AlterOperation;
import com.stratio.crossdata.common.data.AlterOptions;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * AlterTableFactory Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>
 * nov 24, 2014
 * </pre>
 */
public class AlterTableFactoryTest {

    @Before
    public void before() throws Exception {

    }

    /**
     * Method: createHandler(AlterOptions alterOptions)
     */
    @Test
    public void testCreateAddColumnHandeler() throws Exception {
        AlterOptions alterOptions = new AlterOptions(AlterOperation.ADD_COLUMN, Collections.EMPTY_MAP, null);
        assertEquals("The instance is correct", AddColumnHandler.class.getCanonicalName(), AlterTableFactory
                        .createHandler(alterOptions).getClass().getCanonicalName());
    }

    @Test(expected = ExecutionException.class)
    public void testAlterColumnHandler() throws Exception {
        AlterOptions alterOptions = new AlterOptions(AlterOperation.ALTER_COLUMN, Collections.EMPTY_MAP, null);
        assertEquals("The instance is correct", AddColumnHandler.class.getCanonicalName(), AlterTableFactory
                        .createHandler(alterOptions).getClass().getCanonicalName());
    }

    @Test(expected = ExecutionException.class)
    public void testAlterOptionsHandeler() throws Exception {
        AlterOptions alterOptions = new AlterOptions(AlterOperation.ALTER_OPTIONS, Collections.EMPTY_MAP, null);
        assertEquals("The instance is correct", AddColumnHandler.class.getCanonicalName(), AlterTableFactory
                        .createHandler(alterOptions).getClass().getCanonicalName());
    }

    @Test(expected = ExecutionException.class)
    public void testDropColumnHandeler() throws Exception {
        AlterOptions alterOptions = new AlterOptions(AlterOperation.DROP_COLUMN, Collections.EMPTY_MAP, null);
        assertEquals("The instance is correct", AddColumnHandler.class.getCanonicalName(), AlterTableFactory
                        .createHandler(alterOptions).getClass().getCanonicalName());
    }

}
