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

package com.stratio.connector.elasticsearch.core;


import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.elasticsearch.core.configuration.ConnectionConfigurationCreator;
import com.stratio.connector.elasticsearch.core.configuration.ElasticsearchClientConfiguration;
import com.stratio.connector.elasticsearch.core.configuration.SupportedOperationsCreator;
import com.stratio.connector.elasticsearch.core.engine.ElasticsearchMetaProvider;
import com.stratio.connector.elasticsearch.core.engine.ElasticsearchMetadataEngine;
import com.stratio.connector.elasticsearch.core.engine.ElasticsearchQueryEngine;
import com.stratio.connector.elasticsearch.core.engine.ElasticsearchStorageEngine;
import com.stratio.connector.meta.ConnectionConfiguration;
import com.stratio.connector.meta.IMetadataProvider;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.connector.IConnector;
import com.stratio.meta.common.connector.IMetadataEngine;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.connector.IStorageEngine;
import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.security.ICredentials;


/**
 * This class implements the connector for Elasticsearch.
 */
public class ElasticsearchConnector implements IConnector {

	

	/**
	 * The Elasticsearch client.
	 */
	private Client elasticClient = null;
	/**
	 * The connector's configuration.
	 */
	private ElasticsearchClientConfiguration elasticConfiguration = null;
	
	/**
	 * The StorageEngine.
	 */
	private ElasticsearchStorageEngine elasticStorageEngine = null;

	/**
	 * The MetaProvider.
	 */
	private ElasticsearchMetaProvider elasticMetaProvider = null;

	/**
	 * The QueryEngine.
	 */
	private ElasticsearchQueryEngine elasticQueryEngine = null;
	
	/**
	 * The QueryEngine.
	 */
	private ElasticsearchMetadataEngine elasticMetadataEngine = null;
	
	/**
	* The Log.
	*/
	final Logger logger = LoggerFactory.getLogger(this.getClass());

	/**
	 * Create a connection to Elasticsearch. 
	 * The client will be a transportClient by default unless stratio nodeClient is specified.
	 * 
	 * @param credentials
	 *            the security credentials.
	 * @param configuration
	 *            the connection configuration.
	 *
	 * @throws InitializationException
	 *             on failure connection.
	 */
	
	@Override
	public void init(ICredentials credentials, IConfiguration configuration)
			throws InitializationException {
				
				if(!isConnected()){
					
					elasticConfiguration = new ElasticsearchClientConfiguration(configuration);
					if(elasticConfiguration.isNodeClient()){
						createNodeClient(elasticConfiguration, credentials);
					}else createTransportClient(elasticConfiguration, credentials);

				}else {
					throw new InitializationException("connection already exist");
				}

	}
	
	
	/**
	 * Close the Mongo's connection.
	 * 
	 */
	@Override
	public void close() {

		if(elasticClient != null){
			elasticClient.close();
			logger.info("Disconnected from Elasticsearch");
		}
		elasticClient = null;
		elasticStorageEngine = null;
		elasticQueryEngine = null;
		elasticMetaProvider = null;
	}
	

	/**
	 * Return the StorageEngine.
	 * 
	 * @return the StorageEngine
	 */
	@Override
	public IStorageEngine getStorageEngine() {
		createSingletonStorageEngine();
		elasticStorageEngine.setConnection(elasticClient);
		return elasticStorageEngine;

	}

	/**
	 * Return the MetadataProvider.
	 * 
	 * @return the MetadataProvider
	 */
	public IMetadataProvider getMedatadaProvider(){
		createSingletonMetaProvider();
		elasticMetaProvider.setConnection(elasticClient);
		return elasticMetaProvider;
	}

	/**
	 * Return the QueryEngine.
	 * 
	 * @return the QueryEngine
	 */
	@Override
	public IQueryEngine getQueryEngine(){
		createSingletonQueryEngine();
		elasticQueryEngine.setConnection(elasticClient);
		return elasticQueryEngine;
	}

	
	/* (non-Javadoc)
	 * @see com.stratio.meta.common.connector.IConnector#getMetadataEngine()
	 */
	@Override
	public IMetadataEngine getMetadataEngine() throws UnsupportedException {
		createSingletonMetadataEngine();
		elasticMetadataEngine.setStorageEngine((ElasticsearchStorageEngine) getStorageEngine());
		elasticMetadataEngine.setQueryEngine((ElasticsearchQueryEngine) getQueryEngine());
		return elasticMetadataEngine;
	}
	
	
	


	/**
     * Return the supported operations
     *
     * @return the supported operations.
     */
    public Map<Operations, Boolean> getSupportededOperations() {
        return SupportedOperationsCreator.getSupportedOperations();
    }
    
	
    
    /**
     * Return the supported configuration options
     *
     * @return the the supported configuration options.
     */
    public Set<ConnectionConfiguration> getConnectionConfiguration(){
    	return ConnectionConfigurationCreator.getConfiguration();
    }
	
	
	/**
	 * Create a StorageEngine.
	 */
	private void createSingletonStorageEngine() {
		if (elasticStorageEngine == null) {
			elasticStorageEngine = new ElasticsearchStorageEngine();
		}
	}


	/**
	 * Create a QueryEngine.
	 */
	private void createSingletonQueryEngine() {
		if (elasticQueryEngine == null) {
			elasticQueryEngine = new ElasticsearchQueryEngine();
		}
	}

	/**
	 * Create a MetaProvider.
	 */
	private void createSingletonMetaProvider() {
		if (elasticMetaProvider == null) {
			elasticMetaProvider = new ElasticsearchMetaProvider();
		}
	}
	
	/**
	 * Create a MetadataEngine.
	 */
	private void createSingletonMetadataEngine() {
		if (elasticMetadataEngine == null) {
			elasticMetadataEngine = new ElasticsearchMetadataEngine();
		}
		
	}
	
	/**
	* Return the DataStore Name.
	* @return DataStore Name
	*/
	@Override
	public String getDatastoreName() {
		return "Elasticsearch";
	}

	/**
	* The connection status.
	*
	* @return true if the driver's client is not null.
	*/
	@Override
	public boolean isConnected() {

		return elasticClient != null;
		
		
//		boolean connected=false;
//		if ( elasticClient != null ) {
//			ClusterHealthResponse resp = elasticClient.admin().cluster().prepareHealth().execute().actionGet();
//			resp.getStatus();
//			resp.getActiveShards();
//		}
//		return connected;

	}


	 private void createNodeClient(ElasticsearchClientConfiguration elasticConfiguration, ICredentials credentials) throws InitializationException {
		 if(elasticClient == null){
			// TODO settings are overwritten?
			 if (credentials == null) {
				 NodeBuilder nodeBuilder = nodeBuilder();
				 Node node = nodeBuilder.settings(elasticConfiguration.getSettings()).node();	
				 elasticClient = node.client();
				 logger.info("Elasticsearch connection established ");
			 }else throw new InitializationException("Credentials are not supported");
		 }	
	}



		private void createTransportClient(ElasticsearchClientConfiguration elasticConfiguration,
				ICredentials credentials) throws InitializationException {
			
			if (elasticClient == null) {

				if (credentials == null) {
					elasticClient = new TransportClient(elasticConfiguration.getSettings())
					.addTransportAddresses(getTransportAddressFromString(elasticConfiguration.getSeeds()));
					logger.info("Elasticsearch connection established ");
				} else throw new InitializationException("Credentials are not supported");
				
			}
		}
		

		private InetSocketTransportAddress[] getTransportAddressFromString(List<String> seedsStr) throws InitializationException{
			int numSeeds = seedsStr.size();
			if(numSeeds<1) throw new InitializationException("1 o more seeds are required");
				InetSocketTransportAddress[] seeds = new InetSocketTransportAddress[seedsStr.size()];
				int c = 0;
				for (String server : seedsStr) {
					String[] array = server.split(":");
					if (array.length != 2) throw new InitializationException("invalid address => host:port");
					else seeds[c] = new InetSocketTransportAddress(array[0],Integer.decode(array[1]));
					c++;
				}
			return seeds;
		}


	
	

}
