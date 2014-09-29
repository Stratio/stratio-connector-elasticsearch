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

package com.stratio.connector.elasticsearch.ftest.helper;

import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.HOST;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.NODE_TYPE;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.PORT;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexMissingException;

import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.connector.elasticsearch.core.ElasticsearchConnector;
import com.stratio.connector.elasticsearch.core.configuration.ElasticsearchClientConfiguration;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.connector.IConnector;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.metadata.ColumnType;

/**
 * Created by jmgomez on 4/09/14.
 */
public class ESConnectorHelper implements IConnectorHelper {

    protected String SERVER_IP = "192.168.0.3";// "10.200.0.58, 10.200.0.59, 10.200.0.60, 10.200.0.61, 10.200.0.62";
    private String SERVER_PORT = "9300";//,9300,9300,9300,9300";

    private TransportClient auxConection;

    private ClusterName clusterName;

    public ESConnectorHelper(ClusterName clusterName) throws ConnectionException, InitializationException {
        super();
        this.clusterName = clusterName;
        auxConection = new TransportClient(ElasticsearchClientConfiguration.getSettings(getConnectorClusterConfig()))
                .addTransportAddresses(
                        new ElasticsearchClientConfiguration().getTransporAddress(getConnectorClusterConfig()));
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

        return new ConnectorClusterConfig(clusterName, optionsNode);
    }

    @Override
    public ICredentials getICredentials() {
        return mock(ICredentials.class);
    }

    @Override
    public Map<String, Object> recoveredCatalogSettings(String indexName) {

        Map<String, Object> result = new HashMap<>();

        GetSettingsRequest getSettings = new GetSettingsRequest();
        getSettings.indices(indexName);
        getSettings.indicesOptions(IndicesOptions.strictExpandOpen());
        GetSettingsResponse settingsResponse = auxConection.admin().indices().getSettings(getSettings).actionGet();
        for (ObjectObjectCursor<String, Settings> setting : settingsResponse.getIndexToSettings()) {
            result = convertMap(new HashMap(setting.value.getAsMap()));
        }
        return result;
    }

    @Override
    public Collection<ColumnType> getAllSupportedColumnType() {
        Set<ColumnType> allColumntTypes = new HashSet<>();

        allColumntTypes.add(ColumnType.BIGINT);
        allColumntTypes.add(ColumnType.BOOLEAN);
        allColumntTypes.add(ColumnType.DOUBLE);
        allColumntTypes.add(ColumnType.FLOAT);
        allColumntTypes.add(ColumnType.INT);
        allColumntTypes.add(ColumnType.TEXT);
        allColumntTypes.add(ColumnType.VARCHAR);
        return allColumntTypes;
    }

    @Override
    public boolean containsIndex(String catalogName, String collectionName, String indexName) {
        fail("Not yet ES supported");
        return false;
    }

    @Override
    public int countIndexes(String catalogName, String collectionName) {
        fail("Not yet ES supported");
        return 0;
    }

    private Map<String, Object> convertMap(HashMap<String, Object> hashMap) {

        HashMap transformMap = new HashMap();
        for (String key : hashMap.keySet()) {
            String[] aux = key.split("\\.");
            transformMap.put(aux[aux.length - 1], hashMap.get(key));

        }
        return transformMap;
    }

    @Override
    public void refresh(String schema) {
        try {
            if (auxConection != null) {
                auxConection.admin().indices().refresh(new RefreshRequest(schema).force(true)).actionGet();
                auxConection.admin().indices().flush(new FlushRequest(schema).force(true)).actionGet();
            }

        } catch (IndexMissingException e) {
            System.out.println("Index missing");
        }

    }

	@Override
	public boolean isCatalogMandatory() {
	
		return true;
	}

    @Override public boolean isTableMandatory() {
        return false;
    }

	@Override
	public boolean isIndexMandatory() {
		// TODO Auto-generated method stub
		return false;
	}

}
