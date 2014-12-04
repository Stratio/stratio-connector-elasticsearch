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

package com.stratio.connector.elasticsearch.ftest.functionalTestQuery;

import org.junit.Ignore;
import org.junit.Test;

import com.stratio.connector.commons.ftest.functionalTestQuery.GenericOrderByTest;
import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.connector.elasticsearch.ftest.helper.ESConnectorHelper;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.InitializationException;

/**
 * Created by jmgomez on 5/09/14.
 */
public class ESOrderByFT extends GenericOrderByTest {
    protected int getRowToInsert() {
        return 1000;
    }

    @Override
    protected IConnectorHelper getConnectorHelper() {
        ESConnectorHelper esConnectorHelper = null;
        try {
            esConnectorHelper = new ESConnectorHelper(getClusterName());
            return esConnectorHelper;
        } catch (ConnectionException e) {
            e.printStackTrace();
        } catch (InitializationException e) {
            e.printStackTrace();
        }
        return esConnectorHelper;
    }

    @Override @Test @Ignore public void sortDescTest() throws ConnectorException {
        super.sortDescTest();
    }

    @Override @Test @Ignore public void sortTestMultifield() throws ConnectorException {
        super.sortTestMultifield();
    }
}
