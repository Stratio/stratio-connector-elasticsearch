package com.stratio.connector.elasticsearch.core; 

import com.stratio.connector.elasticsearch.core.connection.NodeConnection;
import com.stratio.connector.elasticsearch.core.connection.ConnectionHandle;
import com.stratio.connector.meta.IMetadataProvider;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.connector.IStorageEngine;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After;
import org.junit.runner.RunWith;
import org.mockito.internal.util.reflection.Whitebox;


import java.util.HashMap;
import java.util.Map;

import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/** 
* ElasticsearchConnector Tester. 
* 
* @author <Authors name> 
* @since <pre>ago 28, 2014</pre> 
* @version 1.0 
*/
@RunWith(PowerMockRunner.class)

@PrepareForTest(value = {NodeConnection.class,  ElasticsearchConnector.class})
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
* 
* Method: init(IConfiguration configuration) 
* 
*/ 
@Test
public void testInit() throws Exception {

    IConfiguration iconfiguration = mock(IConfiguration.class);
    elasticsearchConnector.init(iconfiguration);


    ConnectionHandle  connectionHandle  = (ConnectionHandle)Whitebox.getInternalState(elasticsearchConnector, "connectionHandle");
    Object recoveredConfiguration = Whitebox.getInternalState(connectionHandle, "configuration");



    assertNotNull("The configuration is not null",recoveredConfiguration);
    assertEquals("The configuration is correct", iconfiguration, recoveredConfiguration);
    assertNotNull("The connection handle is not null", connectionHandle);
}

/** 
* 
* Method: close() 
* 
*/ 
@Test
public void testConnect() throws Exception {

    ICredentials iCredentials = mock(ICredentials.class);
    ClusterName clusterName = new ClusterName(CLUSTER_NAME);
    Map<String, String> options = new HashMap<>();
    ConnectorClusterConfig config = new ConnectorClusterConfig(clusterName,options);
    ConnectionHandle connectionHandle = mock(ConnectionHandle.class);
    Whitebox.setInternalState(elasticsearchConnector,"connectionHandle",connectionHandle);




    elasticsearchConnector.connect(iCredentials, config);

    verify(connectionHandle,times(1)).createConnection(iCredentials, config);

} 


} 
