package com.stratio.connector.elasticsearch.core.connection; 

import com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/** 
* ConnectionHandle Tester. 
* 
* @author <Authors name> 
* @since <pre>ago 28, 2014</pre> 
* @version 1.0 
*/

@RunWith(PowerMockRunner.class)

@PrepareForTest(value = {  ConnectionHandle.class})

public class ConnectionHandleTest {


    private static final String CLUSTER_NAME = "CLUSTER_NAME";
    private ConnectionHandle connectionHandle = null;
    @Mock private IConfiguration iConfiguration;


    @Before
    public void before() throws Exception {
         connectionHandle = new ConnectionHandle(iConfiguration);

} 

    @After
    public void after() throws Exception {
    }

/** 
* 
* Method: createConnection(String clusterName, Connection connection) 
* 
    */
    @Test
    public void testCreateNodeConnection() throws Exception {



        ICredentials credentials = mock(ICredentials.class);
        Map<String, Object> options = new HashMap<>();
        options.put(ConfigurationOptions.NODE_TYPE.getOptionName(),"true");
        ConnectorClusterConfig config = new ConnectorClusterConfig(new ClusterName(CLUSTER_NAME),options);

        NodeConnection connection = mock(NodeConnection.class);
        whenNew(NodeConnection.class).withArguments(credentials,config).thenReturn(connection);


        connectionHandle.createConnection(credentials,config);

        Map<String,NodeConnection> mapConnection= (Map<String,NodeConnection> )Whitebox.getInternalState(connectionHandle,"connections");

        NodeConnection recoveredConnection = mapConnection.get(CLUSTER_NAME);

        assertNotNull("The connection is not null",recoveredConnection);
        assertEquals("The recoveredConnection is correct", connection,recoveredConnection);


    }

    @Test
    public void testCreateTransportConnection() throws Exception {

        ICredentials credentials = mock(ICredentials.class);
        Map<String, Object> options = new HashMap<>();
        options.put(ConfigurationOptions.NODE_TYPE.getOptionName(),"false");
        ConnectorClusterConfig config = new ConnectorClusterConfig(new ClusterName(CLUSTER_NAME),options);

        TransportConnection connection = mock(TransportConnection.class);
        whenNew(TransportConnection.class).withArguments(credentials,config).thenReturn(connection);



        connectionHandle.createConnection(credentials,config);

        Map<String,Connection> mapConnection= (Map<String,Connection> )Whitebox.getInternalState(connectionHandle,"connections");

        TransportConnection recoveredConnection = (TransportConnection)mapConnection.get(CLUSTER_NAME);

        assertNotNull("The connection is not null",recoveredConnection);
        assertEquals("The recoveredConnection is correct", connection,recoveredConnection);





    }



    @Test
    public void testCloseConnection() throws Exception {

        Map<String,NodeConnection> mapConnection= (Map<String,NodeConnection> )Whitebox.getInternalState(connectionHandle,"connections");
        NodeConnection connection = mock(NodeConnection.class);
        mapConnection.put(CLUSTER_NAME,connection);

        connectionHandle.closeConnection(CLUSTER_NAME);


         assertFalse(mapConnection.containsKey(CLUSTER_NAME));
        verify(connection,times(1)).close();



    }

    @Test
    public void testGetConnection(){
        Map<String,NodeConnection> mapConnection= (Map<String,NodeConnection> )Whitebox.getInternalState(connectionHandle,"connections");
        NodeConnection connection = mock(NodeConnection.class);
        mapConnection.put(CLUSTER_NAME,connection);

        Connection recoveredConnection = connectionHandle.getConnection(CLUSTER_NAME);
        assertNotNull("The connection is not null",recoveredConnection);
        assertSame("The connection is correct",connection,recoveredConnection);


    }

} 
