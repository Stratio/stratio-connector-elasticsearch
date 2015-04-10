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

package com.stratio.connector.elasticsearch.core.ftest.helper;

import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.CLUSTER_NAME;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.HOST;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.NODE_TYPE;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.PORT;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.stratio.crossdata.common.metadata.DataType;
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
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.connector.IConnector;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.security.ICredentials;

/**
 * Created by jmgomez on 4/09/14.
 */
public class ESConnectorHelper implements IConnectorHelper {

    protected String SERVER_IP = "10.200.0.58"; // "192
    // .168.0.3";
    private String SERVER_PORT = "9300";

    // private TransportClient auxConection = null;

    private ClusterName clusterName;

    public ESConnectorHelper(ClusterName clusterName) throws ConnectionException, InitializationException {
        super();
        this.clusterName = clusterName;
        String serverIP = System.getProperty("SERVER_IP");
        if (serverIP != null) {
            SERVER_IP = serverIP;
        }
        String serverPort = System.getProperty("SERVER_PORT");
        if (serverPort != null) {
            SERVER_PORT = serverPort;
        }

    }
    IConnector iConnector = null;
    @Override
    public IConnector getConnector() {

        try {
            if (iConnector ==null) {
                iConnector = new ElasticsearchConnector();
            }
        } catch (InitializationException e) {
            e.printStackTrace();
        }
        return iConnector;
    }

    @Override
    public IConfiguration getConfiguration() {
        return mock(IConfiguration.class);
    }

    @Override
    public ConnectorClusterConfig getConnectorClusterConfig() {
        Map<String, String> optionsNode = new HashMap<>();
        optionsNode.put(NODE_TYPE.getManifestOption(), "false");
        optionsNode.put(HOST.getManifestOption(), SERVER_IP);
        optionsNode.put(PORT.getManifestOption(), SERVER_PORT);
        optionsNode.put(CLUSTER_NAME.getManifestOption(),"statioESCluster");

        return new ConnectorClusterConfig(clusterName, Collections.EMPTY_MAP, optionsNode);
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
        TransportClient auxConection = new TransportClient(
                        ElasticsearchClientConfiguration.getSettings(getConnectorClusterConfig()))
                        .addTransportAddresses(ElasticsearchClientConfiguration
                                        .getTransportAddress(getConnectorClusterConfig()));

        GetSettingsResponse settingsResponse = auxConection.admin().indices().getSettings(getSettings).actionGet();
        auxConection.close();
        for (ObjectObjectCursor<String, Settings> setting : settingsResponse.getIndexToSettings()) {
            result = convertMap(new HashMap(setting.value.getAsMap()));
        }
        return result;
    }

    @Override
    public Collection<ColumnType> getAllSupportedColumnType() {
        Set<ColumnType> allColumntTypes = new HashSet<>();

        allColumntTypes.add(new ColumnType(DataType.BIGINT));
        allColumntTypes.add(new ColumnType(DataType.BOOLEAN));
        allColumntTypes.add(new ColumnType(DataType.DOUBLE));
        allColumntTypes.add(new ColumnType(DataType.FLOAT));
        allColumntTypes.add(new ColumnType(DataType.INT));
        allColumntTypes.add(new ColumnType(DataType.TEXT));
        allColumntTypes.add(new ColumnType(DataType.VARCHAR));
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
        TransportClient auxConection = new TransportClient(
                        ElasticsearchClientConfiguration.getSettings(getConnectorClusterConfig()))
                        .addTransportAddresses(ElasticsearchClientConfiguration
                                        .getTransportAddress(getConnectorClusterConfig()));
        try {

            if (auxConection != null) {
                auxConection.admin().indices().refresh(new RefreshRequest(schema).force(true)).actionGet();
                auxConection.admin().indices().flush(new FlushRequest(schema).force(true)).actionGet();

            }

        } catch (IndexMissingException e) {
        }
        auxConection.close();

    }

    @Override
    public boolean isCatalogMandatory() {

        return true;
    }

    @Override
    public boolean isTableMandatory() {
        return false;
    }

    @Override
    public boolean isIndexMandatory() {
        return false;
    }

    @Override
    public boolean isPKMandatory() {
        return false;
    }

}
