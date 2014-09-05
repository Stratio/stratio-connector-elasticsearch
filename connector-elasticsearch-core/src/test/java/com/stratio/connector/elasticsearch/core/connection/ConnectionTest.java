/*
 * Stratio Meta
 *
 *   Copyright (c) 2014, Stratio, All rights reserved.
 *
 *   This library is free software; you can redistribute it and/or modify it under the terms of the
 *   GNU Lesser General Public License as published by the Free Software Foundation; either version
 *   3.0 of the License, or (at your option) any later version.
 *
 *   This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 *   even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *   Lesser General Public License for more details.
 *
 *   You should have received a copy of the GNU Lesser General Public License along with this library.
 */

package com.stratio.connector.elasticsearch.core.connection;

import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Connection Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>ago 29, 2014</pre>
 */
@RunWith(PowerMockRunner.class)

@PrepareForTest(value = {NodeConnection.class, Client.class, Node.class})
public class ConnectionTest {

    NodeConnection connection;
    @Mock
    ICredentials credentials;
    @Mock
    Node node;
    ConnectorClusterConfig connectorClusterConfig;

    @Mock
    Client elasticClient;

    @Before
    public void before() throws Exception {
        ClusterName clusterName = new ClusterName("CLUSTER NAME");
        Map<String, String> options = new HashMap<>();
        connectorClusterConfig = new ConnectorClusterConfig(clusterName, options);
        connection = new NodeConnection(credentials, connectorClusterConfig);
    }


    /**
     * Method: close()
     */
    @Test
    public void testClose() throws Exception {
        Whitebox.setInternalState(connection, "node", node);


        connection.close();

        verify(node, times(1)).close();
    }


} 
