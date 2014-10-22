/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.stratio.connector.elasticsearch.core;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.elasticsearch.core.connection.ElasticSearchConnectionHandler;
import com.stratio.connector.elasticsearch.core.connection.NodeConnection;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.security.ICredentials;

/**
 * ElasticsearchConnector Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>ago 28, 2014</pre>
 */
@RunWith(PowerMockRunner.class)

@PrepareForTest(value = { NodeConnection.class, ElasticsearchConnector.class })
public class ElasticsearchConnectorTest {

    private static final String CLUSTER_NAME = "CLUSTER_NAME";
    private ElasticsearchConnector elasticsearchConnector = null;

    @Before
    public void before() throws Exception {

        elasticsearchConnector = new ElasticsearchConnector();
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: init(IConfiguration configuration)
     */

    /**
     * Method: close()
     */
    @Test
    public void testConnect() throws Exception, HandlerConnectionException {

        ICredentials iCredentials = mock(ICredentials.class);
        ClusterName clusterName = new ClusterName(CLUSTER_NAME);
        Map<String, String> options = new HashMap<>();
        ConnectorClusterConfig config = new ConnectorClusterConfig(clusterName, options);
        ElasticSearchConnectionHandler connectionHandle = mock(ElasticSearchConnectionHandler.class);
        Whitebox.setInternalState(elasticsearchConnector, "connectionHandler", connectionHandle);

        elasticsearchConnector.connect(iCredentials, config);

        verify(connectionHandle, times(1)).createConnection(iCredentials, config);

    }

}
