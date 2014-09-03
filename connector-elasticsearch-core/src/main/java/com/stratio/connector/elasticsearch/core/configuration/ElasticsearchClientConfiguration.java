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
package com.stratio.connector.elasticsearch.core.configuration;


import java.util.HashMap;

import java.util.Map;


import com.stratio.connector.commons.util.Parser;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import static com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions.*;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.exceptions.InitializationException;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;


/**
 * The configuration for Elasticsearch.
 */
 
public class ElasticsearchClientConfiguration implements IConfiguration{

    /**
     * The parser.
     */
    private Parser parser = new Parser();

	/**
	 * Retrieves the Settings using either the Elasticsearch client configuration or the configuration file.
	 * @param configuration
	 * @throws InitializationException
	 */
	public static Settings getSettings(ConnectorClusterConfig configuration) {

		Map<String,String> setting = new HashMap<String, String>();
        setting.put(NODE_DATA.getOptionName(), addSetting(configuration.getOptions(), NODE_DATA));
        setting.put(NODE_MASTER.getOptionName(),addSetting(configuration.getOptions(),NODE_MASTER));
        setting.put(TRANSPORT_SNIFF.getOptionName(),addSetting(configuration.getOptions(),TRANSPORT_SNIFF));
        setting.put(CLUSTER_NAME.getOptionName(),configuration.getName().getName());

        return ImmutableSettings.settingsBuilder().put(setting).build();
				
	}


    private static String addSetting(Map<String, String> configuration, ConfigurationOptions nodeData) {
        String option;
        if (configuration.containsKey(nodeData.getOptionName())){
            option = (String)configuration.get(nodeData.getOptionName());
        }else{
            option = nodeData.getDefaultValue()[0];
        }
        return option;
    }




    public TransportAddress[] getTransporAddress(ConnectorClusterConfig config) {

        String[] hosts = (String[])parser.hosts(config.getOptions().get(HOST.getOptionName()));
        String[] ports = (String[])parser.ports(config.getOptions().get(PORT.getOptionName()));
        TransportAddress[] transportAddresses = new TransportAddress[1];
        for (int i =0;i<hosts.length;i++){
            transportAddresses[0]=new InetSocketTransportAddress(hosts[i],Integer.decode(ports[i])); //TODO nos pasaran un String con los hosts separados, hay que hacer un parseador
        }
        return transportAddresses;

    }
}