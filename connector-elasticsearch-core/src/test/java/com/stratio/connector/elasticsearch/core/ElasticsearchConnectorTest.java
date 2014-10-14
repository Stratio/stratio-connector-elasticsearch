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
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;

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
