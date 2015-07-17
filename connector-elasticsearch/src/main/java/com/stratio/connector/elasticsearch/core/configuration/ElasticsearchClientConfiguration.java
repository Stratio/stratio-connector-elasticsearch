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

package com.stratio.connector.elasticsearch.core.configuration;

import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.CLUSTER_NAME;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.COERCE;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.DYNAMIC;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.HOST;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.NODE_DATA;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.NODE_MASTER;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.PORT;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.TRANSPORT_SNIFF;

import java.util.HashMap;
import java.util.Map;

import com.stratio.connector.commons.util.PropertyValueRecovered;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;

import com.stratio.crossdata.common.connector.ConnectorClusterConfig;

/**
 * The configuration for Elasticsearch.
 */

public final class ElasticsearchClientConfiguration {

    /**
     * Private constructor.
     */
    private ElasticsearchClientConfiguration() {
    }

    /**
     * Retrieves the Settings using either the Elasticsearch client configuration or the configuration file.
     *
     * @param configuration the configuration
     * @return the settings
     */
    public static Settings getSettings(ConnectorClusterConfig configuration) {

        Map<String, String> setting = new HashMap<String, String>();
        setting.put(NODE_DATA.getElasticSearchOption(), recoverdOptionValue(configuration.getConnectorOptions(), NODE_DATA));
        setting.put(NODE_MASTER.getElasticSearchOption(), recoverdOptionValue(configuration.getConnectorOptions(), NODE_MASTER));
        setting.put(TRANSPORT_SNIFF.getElasticSearchOption(),
                recoverdOptionValue(configuration.getConnectorOptions(), TRANSPORT_SNIFF));
        setting.put(COERCE.getElasticSearchOption(), recoverdOptionValue(configuration.getConnectorOptions(), COERCE));
        setting.put(DYNAMIC.getElasticSearchOption(), recoverdOptionValue(configuration.getConnectorOptions(), DYNAMIC));

        setting.put(CLUSTER_NAME.getElasticSearchOption(),recoverdOptionValue(configuration.getClusterOptions(), CLUSTER_NAME));
        return ImmutableSettings.settingsBuilder().put(setting).build();

    }

    /**
     * this recovered the option value if it is set. It not return the default value..
     *
     * @param configuration the configuration.
     * @param nodeData      the configuration options.
     * @return the actual value of the option.
     */
    private static String recoverdOptionValue(Map<String, String> configuration, ConfigurationOptions nodeData) {
        String option;
        if (configuration.containsKey(nodeData.getManifestOption())) {
            option = configuration.get(nodeData.getManifestOption());
        } else {
            option = nodeData.getDefaultValue()[0];
        }
        return option;
    }

    /**
     * Gets the transport address.
     *
     * @param config the configuration options.
     * @return the transport address
     */
    public static TransportAddress[] getTransportAddress(ConnectorClusterConfig config) throws ExecutionException{

        String[] hosts = PropertyValueRecovered.recoveredValueASArray(String.class, config.getClusterOptions().get(HOST.getManifestOption()));
        String[] ports = PropertyValueRecovered.recoveredValueASArray(String.class, config.getClusterOptions().get(PORT.getManifestOption()));
        TransportAddress[] transportAddresses = new TransportAddress[hosts.length];

        for (int i = 0; i < hosts.length; i++) {
            transportAddresses[i] = new InetSocketTransportAddress(hosts[i], Integer.decode(ports[i]));
        }

        return transportAddresses;

    }
}