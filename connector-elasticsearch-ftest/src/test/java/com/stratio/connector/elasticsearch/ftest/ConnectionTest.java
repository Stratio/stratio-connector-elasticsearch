/**
* Copyright (C) 2014 Stratio (http://stratio.com)
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.stratio.connector.elasticsearch.ftest;

import com.stratio.connector.elasticsearch.core.ElasticsearchConnector;
import com.stratio.connector.elasticsearch.core.connection.ConnectionHandle;
import com.stratio.connector.meta.ConfigurationImplem;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta2.common.data.ClusterName;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.indices.IndexMissingException;
import org.junit.After;
import org.junit.Before;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.HashMap;
import java.util.Map;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.*;

public class ConnectionTest {

    public static final ClusterName CLUSTER_NODE_NAME = new ClusterName("FUNCTIONAL_NODE_TEST_ES");
    public static final ClusterName CLUSTER_TRANSPORT_NAME = new ClusterName("FUNCTIONAL_TRANSPORT_TEST_ES");

    protected final String COLLECTION = getClass().getSimpleName();
    protected final String CATALOG = "functionaltest";
    protected final Long MILLIS = 1200l;
    
    
    /**
     * The elasticsearch elasticClient.
     */
    protected Client nodeClient = null;
    protected Client transportClient = null;
    protected ElasticsearchConnector stratioElasticConnector = null;


    protected String SERVER_NODE_IP = "localhost";//"172.19.0.77"
    protected String SERVER_TRANSPORT_IP = "localhost";//"172.19.0.77"
    private String SERVER_NODE_PORT = "9300";
    private String SERVER_TRANSPORT_PORT = "9301";



    @Before
    public void setUp() throws InitializationException, ConnectionException {



        stratioElasticConnector = new ElasticsearchConnector();
        stratioElasticConnector.init(new ConfigurationImplem());


       System.out.println("Create node connection");
        stratioElasticConnector.connect(null, createNodeConnection());

        System.out.println("************** Create transport connection");
        ConnectorClusterConfig transportConnection = createTransportConnection();
        stratioElasticConnector.connect(null, transportConnection);

        ConnectionHandle connectionHandle=  (ConnectionHandle)Whitebox.getInternalState(stratioElasticConnector,"connectionHandle");
        nodeClient = (Client) connectionHandle.getConnection(CLUSTER_NODE_NAME.getName()).getClient();
        transportClient = (Client) connectionHandle.getConnection(CLUSTER_TRANSPORT_NAME.getName()).getClient();

        deleteSet(CATALOG);
        System.out.println(CATALOG+"/"+COLLECTION);
    }

    private ConnectorClusterConfig createNodeConnection() {
        Map<String, String> optionsNode = new HashMap<>();
        optionsNode.put(NODE_TYPE.getOptionName(), "true");
        return new ConnectorClusterConfig(CLUSTER_NODE_NAME,optionsNode);
    }

    private ConnectorClusterConfig createTransportConnection() {
        Map<String, String> optionsNode = new HashMap<>();
        optionsNode.put(NODE_TYPE.getOptionName(), "false");
        optionsNode.put(HOST.getOptionName(),SERVER_TRANSPORT_IP);
        optionsNode.put(PORT.getOptionName(), SERVER_TRANSPORT_PORT);
        return new ConnectorClusterConfig(CLUSTER_TRANSPORT_NAME,optionsNode);
    }


    private void deleteSet(String catalog){


        try {
            if (nodeClient != null) nodeClient.admin().indices().delete(new DeleteIndexRequest(catalog)).actionGet();
        }catch(IndexMissingException e){
            System.out.println("Index not exist");
        }
        try{
            if (transportClient!=null) transportClient.admin().indices().delete(new DeleteIndexRequest(catalog)).actionGet();
        }catch(IndexMissingException e){
            System.out.println("Index not exist");
        }


    }


    protected void refresh(String catalog){

    	   try {
               if (nodeClient != null) nodeClient.admin().indices().flush(new FlushRequest(catalog)).actionGet();
           }catch(IndexMissingException e){
               System.out.println("Index missing");
           }
        try{
            if (transportClient!=null) transportClient.admin().indices().flush(new FlushRequest(catalog)).actionGet();
        }catch(IndexMissingException e){
            System.out.println("Index missing");
        }

    }
    
    @After
    public void tearDown() throws ConnectionException {
        deleteSet(CATALOG);
        stratioElasticConnector.close(CLUSTER_NODE_NAME);
        stratioElasticConnector.close(CLUSTER_TRANSPORT_NAME);

    }

}
