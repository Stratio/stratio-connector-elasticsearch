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

package com.stratio.connector.elasticsearch.core.configuration;

import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.ClusterName;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.*;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

/**
 * ElasticsearchClientConfiguration Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>oct 14, 2014</pre>
 */
public class ElasticsearchClientConfigurationTest {

    private static final ClusterName THE_CLUSTER_NAME = new ClusterName("cluster_name");

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: getSettings(ConnectorClusterConfig configuration)
     */
    @Test
    public void testGetSettings() throws Exception {


        Map<String, String> connectorOptions = new HashMap<>();
        connectorOptions.put(NODE_DATA.getManifestOption(), "true");
        connectorOptions.put(NODE_MASTER.getManifestOption(), "true");

        Map<String, String> clusterOptions = new HashMap<>();
        clusterOptions.put(HOST.getManifestOption(), "localhost");
        clusterOptions.put(PORT.getManifestOption(), "9301");


        ConnectorClusterConfig configuration = new ConnectorClusterConfig(THE_CLUSTER_NAME, connectorOptions,clusterOptions);

        Settings result = ElasticsearchClientConfiguration.getSettings(configuration);

        assertNotNull("Result is not null", result);
        assertEquals("The node must be true", "true", result.get(NODE_DATA.getElasticSearchOption()));
        assertEquals("The node master must be true", "true", result.get(NODE_MASTER.getElasticSearchOption()));

        assertEquals("The transport sniff must be "+TRANSPORT_SNIFF.getDefaultValue()[0], TRANSPORT_SNIFF
                        .getDefaultValue()[0],
                result.get(TRANSPORT_SNIFF.getElasticSearchOption()));

        assertEquals("The cluster name sniff must be Cluster Name", CLUSTER_NAME.getDefaultValue()[0],
                result.get(CLUSTER_NAME.getElasticSearchOption()));

        assertEquals("The cluster name sniff must be "+COERCE.getDefaultValue()[0], COERCE.getDefaultValue()[0],
                result.get(COERCE.getElasticSearchOption()));

        assertEquals("The cluster name sniff must be "+DYNAMIC.getDefaultValue()[0], DYNAMIC.getDefaultValue()[0],
                result.get(DYNAMIC.getElasticSearchOption()));

    }

    /**
     * Method: getTransportAddress(ConnectorClusterConfig config)
     */
    @Test
    public void testGetTransporAddress() throws Exception {
        Map<String, String> clusterOptions = new HashMap<>();
        clusterOptions.put(HOST.getManifestOption(), "10.200.0.58,10.200.0.59,10.200.0.60");
        clusterOptions.put(PORT.getManifestOption(), "2800,2802,2809");
        ConnectorClusterConfig configuration = new ConnectorClusterConfig(THE_CLUSTER_NAME,
                Collections.EMPTY_MAP, clusterOptions);

        TransportAddress[] result = ElasticsearchClientConfiguration.getTransportAddress(configuration);
        assertNotNull("The result is not null", result);
        assertEquals("The result  number is correct", 3, result.length);
        assertEquals("The first address is correct", "inet[/10.200.0.58:2800]", result[0].toString());
        assertEquals("The second address is correct", "inet[/10.200.0.59:2802]", result[1].toString());
        assertEquals("The third address is correct", "inet[/10.200.0.60:2809]", result[2].toString());

    }

} 
