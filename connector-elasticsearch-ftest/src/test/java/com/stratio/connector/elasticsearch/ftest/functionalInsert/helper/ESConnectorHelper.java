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

package com.stratio.connector.elasticsearch.ftest.functionalInsert.helper;

import com.stratio.connector.elasticsearch.core.ElasticsearchConnector;
import com.stratio.connector.elasticsearch.core.configuration.ElasticsearchClientConfiguration;
import com.stratio.connector.elasticsearch.core.connection.ElasticSearchConnectionHandler;
import com.stratio.connector.elasticsearch.ftest.helper.IConnectorHelper;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.connector.IConnector;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.indices.IndexMissingException;

import java.util.HashMap;
import java.util.Map;

import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.*;
import static org.mockito.Mockito.mock;

/**
 * Created by jmgomez on 4/09/14.
 */
public class ESConnectorHelper implements IConnectorHelper{

private ElasticSearchConnectionHandler connectorHandle;

    protected String SERVER_IP = "10.200.0.58,10.200.0.59,10.200.0.60";//"localhost";//"172.19.0.77"
     private String SERVER_PORT = "9300,9300,9300";


    private TransportClient auxConection;

    private ClusterName clusterName;

    public  ESConnectorHelper(ClusterName clusterName) throws ConnectionException, InitializationException {
        super();
        this.clusterName =clusterName;
        auxConection = new TransportClient(ElasticsearchClientConfiguration.getSettings(getConnectorClusterConfig()))
                .addTransportAddresses(new ElasticsearchClientConfiguration().getTransporAddress(getConnectorClusterConfig()));
    }

    @Override
    public IConnector getConnector() {
           return new ElasticsearchConnector();
    }

    @Override
    public IConfiguration getConfiguration() {
        return mock(IConfiguration.class);
    }

    @Override
    public ConnectorClusterConfig getConnectorClusterConfig() {
        Map<String, String> optionsNode = new HashMap<>();
        optionsNode.put(NODE_TYPE.getOptionName(), "false");
        optionsNode.put(HOST.getOptionName(), SERVER_IP);
        optionsNode.put(PORT.getOptionName(), SERVER_PORT);
        return new ConnectorClusterConfig(clusterName,optionsNode);
    }

    @Override
    public ICredentials getICredentials() {
        return mock(ICredentials.class);
    }

    @Override
    public void deleteSet(String schema) {
            try {
                if (auxConection != null) auxConection.admin().indices().delete(new DeleteIndexRequest(schema)).actionGet();
            } catch (IndexMissingException e) {
                System.out.println("Index not exist");
            }

    }

    @Override
    public void refresh(String schema) {
       try{
            if (auxConection !=null) {
                auxConection.admin().indices().refresh(new RefreshRequest(schema).force(true)).actionGet();
                auxConection.admin().indices().flush(new FlushRequest(schema).force(true)).actionGet();
            }
        }catch(IndexMissingException e){
            System.out.println("Index missing");
        }


    }

}
