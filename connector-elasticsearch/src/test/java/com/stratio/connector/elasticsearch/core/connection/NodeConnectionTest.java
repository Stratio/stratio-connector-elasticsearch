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

import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.security.ICredentials;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.Map;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * NodeConnection Tester.
 *
 * @author <Authors name>
 * @version 1.0
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(NodeBuilder.class)
public class NodeConnectionTest {

    @Mock Client client;
    NodeConnection nodeConnection;
    @Mock Node node;

    @Before
    public void before() throws Exception {
        ICredentials credentials = mock(ICredentials.class);

        Map<String, String> options = Collections.EMPTY_MAP;

        ConnectorClusterConfig configuration = new ConnectorClusterConfig(new ClusterName("CLUSTER_NAME"), options,
                options);
        mockStatic(NodeBuilder.class);
        NodeBuilder nodeBuilder = mock(NodeBuilder.class);
        when(nodeBuilder.settings(any(Settings.class))).thenReturn(nodeBuilder);
        when(nodeBuilder.node()).thenReturn(node);
        when(node.client()).thenReturn(client);
        when(NodeBuilder.nodeBuilder()).thenReturn(nodeBuilder);

        nodeConnection = new NodeConnection(credentials, configuration);

        assertNotNull("The connection is not null", Whitebox.getInternalState(nodeConnection, "elasticClient"));

    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: close()
     */
    @Test
    public void testClose() throws Exception {

        Whitebox.setInternalState(nodeConnection, "elasticClient", client);
        Whitebox.setInternalState(nodeConnection, "node", node);

        nodeConnection.close();

        verify(node, times(1)).close();
        assertNull("The connection is null", Whitebox.getInternalState(nodeConnection, "elasticClient"));
        assertNull("The node is null", Whitebox.getInternalState(nodeConnection, "node"));

    }

} 
