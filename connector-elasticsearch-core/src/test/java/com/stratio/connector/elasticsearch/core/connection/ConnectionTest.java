package com.stratio.connector.elasticsearch.core.connection; 

import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.junit.Test;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** 
* Connection Tester. 
* 
* @author <Authors name> 
* @since <pre>ago 29, 2014</pre> 
* @version 1.0 
*/
@RunWith(PowerMockRunner.class)

@PrepareForTest(value = {  NodeConnection.class,Client.class, Node.class})
public class ConnectionTest {

    NodeConnection connection;
    @Mock ICredentials credentials;
    @Mock Node node;
    ConnectorClusterConfig connectorClusterConfig;

    @Mock  Client elasticClient;

@Before
public void before() throws Exception {
    ClusterName clusterName = new ClusterName("CLUSTER NAME");
    Map<String, Object> options = new HashMap<>();
    connectorClusterConfig = new ConnectorClusterConfig(clusterName, options);
    connection = new NodeConnection(credentials,connectorClusterConfig);
} 






    /**
* 
* Method: close() 
* 
*/ 
@Test
public void testClose() throws Exception {
    Whitebox.setInternalState(connection,"node",node);


    connection.close();

    verify(node,times(1)).close();
} 


} 
