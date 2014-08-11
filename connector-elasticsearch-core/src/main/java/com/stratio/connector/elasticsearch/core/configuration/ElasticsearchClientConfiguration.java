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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import com.stratio.connector.meta.ConfigurationImplem;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.exceptions.InitializationException;


/**
 * The configuration for Elasticsearch.
 */
 
public class ElasticsearchClientConfiguration implements IConfiguration{
	
	/**
     * The Elasticsearch driver settings
     */
	private Settings elasticsearchSettings = null;
	
	/**
     * List of seeds
     */
	private List<String> seeds = null;

	/**
     * Type of Elasticsearch client
     */
	private boolean isNodeClient;



	/**
	 * Initializes the Elasticsearch client configuration from a map based configuration
	 * @param iconfiguration 
	 * @throws InitializationException
	 */
	public ElasticsearchClientConfiguration(IConfiguration iconfiguration) throws InitializationException{
		isNodeClient = false;
		ConfigurationImplem configuration = (ConfigurationImplem) iconfiguration;
		configureSeeds(configuration);
		configureClientOptions(configuration);
	}

	/**
	 * Retrieves the seeds from the Elasticsearch client configuration
	 * @param iconfiguration 
	 * @throws InitializationException
	 */
	private void configureSeeds (ConfigurationImplem configuration) throws InitializationException{
		
		if( (seeds = configuration.getSeeds()) == null ){
			throw new InitializationException("there is no seeds");
		}

	}
	/**
	 * Retrieves the Settings using either the Elasticsearch client configuration or the configuration file.
	 * @param iconfiguration 
	 * @throws InitializationException
	 */
	public void configureClientOptions(ConfigurationImplem configuration) {

		Map<String,String> confSettings = configuration.getMapProperties();
		Map<String,String> confSettingsElasticsearch = new HashMap<String, String>();
		
		 for (Entry<String, String> e: confSettings.entrySet()) {
			 	String newKey = e.getKey().substring("elasticsearch.".length());
			 	if(!newKey.startsWith("stratio.")) confSettingsElasticsearch.put(newKey, e.getValue());
			 	else configureClientType(e.getValue());
			 	
	     }

		elasticsearchSettings = ImmutableSettings.settingsBuilder().put(confSettingsElasticsearch).build();
				
	}
	
	/**
	 * Sets the client's type.
	 * @param isNodeClient 
	 */
	private void configureClientType(String isNodeClient){
		this.isNodeClient = Boolean.parseBoolean(isNodeClient);
	}
	
	
	/**
	 * Returns the Elasticsearch driver settings
	 * @return the Elasticsearch driver settings
	 */
	public Settings getSettings() {
		return elasticsearchSettings;
	}
	/**
	 * Returns the list of seeds
	 * @return the seeds
	 */
	public List<String> getSeeds(){
		return seeds;
	}
	/**
	 * Returns the type of client
	 * @return true for a node client, false for a transport client.
	 */
	public boolean isNodeClient(){
		return isNodeClient;
	}
	
	//	public  ConnectorConfigurationOptions getConnectorConfigurationOptions(){
	//		
	//	}

}