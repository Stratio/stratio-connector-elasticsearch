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
package com.stratio.connector.elasticsearch.core.configuration;

import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.CLUSTER_NAME;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.HOST;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.NODE_DATA;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.NODE_MASTER;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.PORT;
import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.TRANSPORT_SNIFF;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;

import com.stratio.connector.commons.util.ConnectorParser;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.exceptions.InitializationException;

/**
 * The configuration for Elasticsearch.
 */

public class ElasticsearchClientConfiguration /*implements IConfiguration*/ {


    /**
     * Retrieves the Settings using either the Elasticsearch client configuration or the configuration file.
     *
     * @param configuration
     * @throws InitializationException
     */
    public static Settings getSettings(ConnectorClusterConfig configuration) {

        Map<String, String> setting = new HashMap<String, String>();
        setting.put(NODE_DATA.getOptionName(), addSetting(configuration.getOptions(), NODE_DATA));
        setting.put(NODE_MASTER.getOptionName(), addSetting(configuration.getOptions(), NODE_MASTER));
        setting.put(TRANSPORT_SNIFF.getOptionName(), addSetting(configuration.getOptions(), TRANSPORT_SNIFF));
        setting.put(CLUSTER_NAME.getOptionName(), configuration.getName().getName());
        setting.put("index.mapping.coerce", "false");
        setting.put("index.mapper.dynamic", "false");
        //setting.put("index.mapping.ignore_malformed", "false");

        return ImmutableSettings.settingsBuilder().put(setting).build();

    }

    private static String addSetting(Map<String, String> configuration, ConfigurationOptions nodeData) {
        String option;
        if (configuration.containsKey(nodeData.getOptionName())) {
            option = (String) configuration.get(nodeData.getOptionName());
        } else {
            option = nodeData.getDefaultValue()[0];
        }
        return option;
    }

    public TransportAddress[] getTransporAddress(ConnectorClusterConfig config) {

        String[] hosts =  ConnectorParser.hosts(config.getOptions().get(HOST.getOptionName()));
        String[] ports =  ConnectorParser.ports(config.getOptions().get(PORT.getOptionName()));
        TransportAddress[] transportAddresses = new TransportAddress[1];
        for (int i = 0; i < hosts.length; i++) {
            transportAddresses[0] = new InetSocketTransportAddress(hosts[i], Integer.decode(
                    ports[i])); //TODO nos pasaran un String con los hosts separados, hay que hacer un parseador
        }
        return transportAddresses;

    }
}