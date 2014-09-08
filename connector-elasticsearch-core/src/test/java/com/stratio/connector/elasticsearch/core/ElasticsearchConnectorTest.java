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

import com.stratio.connector.elasticsearch.core.connection.ElasticSearchConnectionHandle;
import com.stratio.connector.elasticsearch.core.connection.NodeConnection;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

/**
 * ElasticsearchConnector Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>ago 28, 2014</pre>
 */
@RunWith(PowerMockRunner.class)

@PrepareForTest(value = {NodeConnection.class, ElasticsearchConnector.class})
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
    @Test
    public void testInit() throws Exception {

        IConfiguration iconfiguration = mock(IConfiguration.class);
        elasticsearchConnector.init(iconfiguration);


        ElasticSearchConnectionHandle connectionHandle = (ElasticSearchConnectionHandle) Whitebox.getInternalState(elasticsearchConnector, "connectionHandle");
        Object recoveredConfiguration = Whitebox.getInternalState(connectionHandle, "configuration");


        assertNotNull("The configuration is not null", recoveredConfiguration);
        assertEquals("The configuration is correct", iconfiguration, recoveredConfiguration);
        assertNotNull("The connection handle is not null", connectionHandle);
    }

    /**
     * Method: close()
     */
    @Test
    public void testConnect() throws Exception {

        ICredentials iCredentials = mock(ICredentials.class);
        ClusterName clusterName = new ClusterName(CLUSTER_NAME);
        Map<String, String> options = new HashMap<>();
        ConnectorClusterConfig config = new ConnectorClusterConfig(clusterName, options);
        ElasticSearchConnectionHandle connectionHandle = mock(ElasticSearchConnectionHandle.class);
        Whitebox.setInternalState(elasticsearchConnector, "connectionHandle", connectionHandle);


        elasticsearchConnector.connect(iCredentials, config);

        verify(connectionHandle, times(1)).createConnection(iCredentials, config);

    }


} 