package com.stratio.connector.elasticsearch.core.connection; 

import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.HOST;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.PORT;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
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
* NodeConnection Tester. 
* 
* @author <Authors name> 
* @since <pre>oct 14, 2014</pre> 
* @version 1.0 
*/
@RunWith(PowerMockRunner.class)
public class NodeConnectionTest {

    @Mock Client client;
    NodeConnection nodeConnection;
    @Mock Node node;

@Before
public void before() throws Exception {
    ICredentials credentials = mock(ICredentials.class);


    Map<String, String> options = Collections.EMPTY_MAP;

    ConnectorClusterConfig configuration = new ConnectorClusterConfig(new ClusterName("CLUSTER_NAME"),options);
    nodeConnection = new NodeConnection(credentials,configuration);


    assertNotNull("The connection is not null", Whitebox.getInternalState(nodeConnection, "elasticClient"));
    assertTrue("The connection is  connected", (Boolean) Whitebox.getInternalState(nodeConnection,
            "isConnect"));
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: close() 
* 
*/ 
@Test
public void testClose() throws Exception {

    Whitebox.setInternalState(nodeConnection, "elasticClient", client);
    Whitebox.setInternalState(nodeConnection, "node", node);

    nodeConnection.close();

    verify(node,times(1)).close();
    assertNull("The connection is null",Whitebox.getInternalState(nodeConnection, "elasticClient"));
    assertNull("The node is null",Whitebox.getInternalState(nodeConnection, "node"));
    assertFalse("The connection is not connected", (Boolean)Whitebox.getInternalState(nodeConnection,
            "isConnect"));
} 



} 
