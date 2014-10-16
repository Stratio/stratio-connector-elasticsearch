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

package com.stratio.connector.elasticsearch.core.connection;

import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.HOST;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.PORT;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After;
import org.junit.runner.RunWith;

import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;

/** 
* TransportConnection Tester. 
* 
* @author <Authors name> 
* @since <pre>oct 14, 2014</pre> 
* @version 1.0 
*/
@RunWith(PowerMockRunner.class)
public class TransportConnectionTest {

    @Mock Client client;
    TransportConnection transportConnection;
@Before
public void before() throws Exception {
    ICredentials credentials = mock(ICredentials.class);


    Map<String, String> options = new HashMap<>();
    options.put(HOST.getOptionName(), "10.200.0.58,10.200.0.59,10.200.0.60");
    options.put(PORT.getOptionName(), "2800,2802,2809");
    ConnectorClusterConfig configuration = new ConnectorClusterConfig(new ClusterName("CLUSTER_NAME"),options);
    transportConnection = new TransportConnection(credentials,configuration);


    assertNotNull("The connection is not null", Whitebox.getInternalState(transportConnection, "elasticClient"));
    assertTrue("The connection is  connected", (Boolean) Whitebox.getInternalState(transportConnection,
            "isConnect"));
}





/** 
*
* Method: close() 
* 
*/ 
@Test
public void testClose() throws Exception {

    Whitebox.setInternalState(transportConnection, "elasticClient", client);
    transportConnection.close();

    verify(client,times(1)).close();
   assertNull("The connection is null",Whitebox.getInternalState(transportConnection, "elasticClient"));
    assertFalse("The connection is not connected", (Boolean)Whitebox.getInternalState(transportConnection,
            "isConnect"));
} 




} 
