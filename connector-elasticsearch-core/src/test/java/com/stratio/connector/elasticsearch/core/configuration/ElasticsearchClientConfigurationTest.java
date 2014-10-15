package com.stratio.connector.elasticsearch.core.configuration; 

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After;

import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta2.common.data.ClusterName;

import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.CLUSTER_NAME;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.COERCE;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.DYNAMIC;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.HOST;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.NODE_DATA;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.NODE_MASTER;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.PORT;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.TRANSPORT_SNIFF;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

import junit.framework.Assert;

/** 
* ElasticsearchClientConfiguration Tester. 
* 
* @author <Authors name> 
* @since <pre>oct 14, 2014</pre> 
* @version 1.0 
*/ 
public class ElasticsearchClientConfigurationTest {

    private static final ClusterName THE_CLUSTER_NAME = new ClusterName("CLUSTER_NAME");


@Before
public void before() throws Exception { 
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: getSettings(ConnectorClusterConfig configuration) 
* 
*/ 
@Test
public void testGetSettings() throws Exception {

    Map<String, String> options = new HashMap<>();
    options.put(NODE_DATA.getOptionName(), "true");
    options.put(NODE_MASTER.getOptionName(), "true");
    ConnectorClusterConfig configuration = new ConnectorClusterConfig(THE_CLUSTER_NAME,options);


   Settings result =  ElasticsearchClientConfiguration.getSettings(configuration);



    assertNotNull("Result is not null",result);
    assertEquals("The node data is correct","true",result.get(NODE_DATA.getOptionName()));
    assertEquals("The node master is correct","true",result.get(NODE_MASTER.getOptionName()));

    assertEquals("The transport sniff is correct",TRANSPORT_SNIFF.getDefaultValue()[0],
            result.get(TRANSPORT_SNIFF.getOptionName()));

     assertEquals("The cluster name sniff is correct","CLUSTER_NAME",
                    result.get(CLUSTER_NAME.getOptionName()));

    assertEquals("The cluster name sniff is correct",COERCE.getDefaultValue()[0],
            result.get(COERCE.getOptionName()));

    assertEquals("The cluster name sniff is correct",DYNAMIC.getDefaultValue()[0],
            result.get(DYNAMIC.getOptionName()));





} 

/**
* 
* Method: getTransportAddress(ConnectorClusterConfig config)
* 
*/ 
@Test
public void testGetTransporAddress() throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(HOST.getOptionName(), "10.200.0.58,10.200.0.59,10.200.0.60");
    options.put(PORT.getOptionName(), "2800,2802,2809");
    ConnectorClusterConfig configuration = new ConnectorClusterConfig(THE_CLUSTER_NAME,options);


   TransportAddress[] result =  ElasticsearchClientConfiguration.getTransportAddress(configuration);
   assertNotNull("The result is not null",result);
    assertEquals("The result  number is correct",3,result.length);
    assertEquals("The first address is correct","inet[/10.200.0.58:2800]",result[0].toString());
    assertEquals("The second address is correct","inet[/10.200.0.59:2802]",result[1].toString());
    assertEquals("The third address is correct","inet[/10.200.0.60:2809]",result[2].toString());

} 




} 