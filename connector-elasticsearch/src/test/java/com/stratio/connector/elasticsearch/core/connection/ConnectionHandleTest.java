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

package com.stratio.connector.elasticsearch.core.connection;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.security.ICredentials;
import org.elasticsearch.client.transport.TransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * ConnectionHandle Tester.
 *
 * @author <Authors name>
 * @version 1.0
 */

@RunWith(PowerMockRunner.class)

@PrepareForTest(value = { ElasticSearchConnectionHandler.class, TransportClient.class })

public class ConnectionHandleTest {

    private static final String CLUSTER_NAME = "cluster_name";
    private ElasticSearchConnectionHandler connectionHandle = null;
    @Mock
    private IConfiguration iConfiguration;

    @Before
    public void before() throws Exception {
        connectionHandle = new ElasticSearchConnectionHandler(iConfiguration);

    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: createConnection(String clusterName, Connection connection)
     */
    @Test
    public void testCreateNodeConnection() throws Exception {

        ICredentials credentials = mock(ICredentials.class);
        Map<String, String> connectionOptios = new HashMap<>();
        connectionOptios.put(ConfigurationOptions.NODE_TYPE.getManifestOption(), "true");
        ConnectorClusterConfig config = new ConnectorClusterConfig(new ClusterName(CLUSTER_NAME), connectionOptios,
                Collections.EMPTY_MAP);

        NodeConnection connection = mock(NodeConnection.class);
        whenNew(NodeConnection.class).withArguments(credentials, config).thenReturn(connection);
        when(connection.isConnected()).thenReturn(true);

        connectionHandle.createConnection(credentials, config);

        Map<String, NodeConnection> mapConnection = (Map<String, NodeConnection>) Whitebox
                .getInternalState(connectionHandle, "connections");

        NodeConnection recoveredConnection = mapConnection.get(CLUSTER_NAME);

        assertNotNull("The connection is not null", recoveredConnection);
        assertEquals("The recoveredConnection is correct", connection, recoveredConnection);

    }

    @Test
    public void testCreateTransportConnection() throws Exception {

        ICredentials credentials = mock(ICredentials.class);

        Map<String, String> options = new HashMap<>();
        options.put(ConfigurationOptions.NODE_TYPE.getManifestOption(), "false");

        ConnectorClusterConfig config = new ConnectorClusterConfig(new ClusterName(CLUSTER_NAME), options, Collections.EMPTY_MAP);

        TransportConnection connection = mock(TransportConnection.class);
        whenNew(TransportConnection.class).withArguments(credentials, config).thenReturn(connection);
        when(connection.isConnected()).thenReturn(true);

        connectionHandle.createConnection(credentials, config);

        Map<String, Connection> mapConnection = (Map<String, Connection>) Whitebox
                .getInternalState(connectionHandle, "connections");

        TransportConnection recoveredConnection = (TransportConnection) mapConnection.get(CLUSTER_NAME);

        assertNotNull("The connection is not null", recoveredConnection);
        assertEquals("The recoveredConnection is correct", connection, recoveredConnection);

    }

    @Test
    public void testCloseConnection() throws Exception {

        Map<String, NodeConnection> mapConnection = (Map<String, NodeConnection>) Whitebox
                .getInternalState(connectionHandle, "connections");
        NodeConnection connection = mock(NodeConnection.class);
        mapConnection.put(CLUSTER_NAME, connection);

        connectionHandle.closeConnection(CLUSTER_NAME);

        assertFalse("The connections is not in the connectionMap", mapConnection.containsKey(CLUSTER_NAME));
        verify(connection, times(1)).close();

    }

    @Test
    public void testGetConnection() throws ExecutionException {
        Map<String, NodeConnection> mapConnection = (Map<String, NodeConnection>) Whitebox
                .getInternalState(connectionHandle, "connections");
        NodeConnection connection = mock(NodeConnection.class);
        mapConnection.put(CLUSTER_NAME, connection);

        Connection recoveredConnection = connectionHandle.getConnection(CLUSTER_NAME);
        assertNotNull("The connection is not null", recoveredConnection);
        assertSame("The connection is correct", connection, recoveredConnection);

    }

} 
