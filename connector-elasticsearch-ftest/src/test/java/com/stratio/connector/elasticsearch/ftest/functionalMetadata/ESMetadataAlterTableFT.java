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

package com.stratio.connector.elasticsearch.ftest.functionalMetadata;

import org.junit.Ignore;
import org.junit.Test;

import com.stratio.connector.commons.ftest.functionalMetadata.GenericMetadataAlterTableFT;
import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.connector.elasticsearch.ftest.helper.ESConnectorHelper;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.InitializationException;

/**
 * Created by jmgomez on 24/11/14.
 */
public class ESMetadataAlterTableFT extends GenericMetadataAlterTableFT {

    ESConnectorHelper esConnectorHelper = null;

    @Override @Test @Ignore public void dropColumnFT() throws ConnectorException {
        super.dropColumnFT();
    }

    @Override
    protected IConnectorHelper getConnectorHelper() {
        try {
            if (esConnectorHelper == null) {
                esConnectorHelper = new ESConnectorHelper(getClusterName());
            }

            return esConnectorHelper;
        } catch (ConnectionException e) {
            e.printStackTrace();
        } catch (InitializationException e) {
            e.printStackTrace();
        }
        return esConnectorHelper;
    }
}