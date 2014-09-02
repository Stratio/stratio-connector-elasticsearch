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


import java.util.List;
import java.util.Map;
import java.util.Set;

import com.stratio.connector.elasticsearch.core.connection.ConnectionHandle;
import com.stratio.connector.elasticsearch.core.engine.ElasticsearchMetadataEngine;
import com.stratio.meta.common.connector.*;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta2.common.data.ClusterName;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.elasticsearch.core.configuration.ConnectionConfigurationCreator;
import com.stratio.connector.elasticsearch.core.configuration.SupportedOperationsCreator;
import com.stratio.connector.elasticsearch.core.engine.ElasticsearchMetaProvider;
import com.stratio.connector.elasticsearch.core.engine.ElasticsearchQueryEngine;
import com.stratio.connector.elasticsearch.core.engine.ElasticsearchStorageEngine;
import com.stratio.connector.meta.ConnectionConfiguration;
import com.stratio.connector.meta.IMetadataProvider;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.security.ICredentials;


/**
 * This class implements the connector for Elasticsearch.
 */
public class ElasticsearchConnector implements IConnector {



    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The connectionHandle.
     */
    private ConnectionHandle connectionHandle = null;
	



    @Override
    public String getConnectorName() {
        return null; //TODO
    }

    /**
     * Return the DataStore Name.
     * @return DataStore Name
     */
    @Override
    public String[] getDatastoreName() {
        return new String[] {"Elasticsearch"}; //TODO
    }

    /**
     * Return the StorageEngine.
     *
     * @return the StorageEngine
     */
    @Override
    public IStorageEngine getStorageEngine() {

        return new ElasticsearchStorageEngine(connectionHandle);

    }

    /**
     * Return the MetadataProvider.
     *
     * @return the MetadataProvider
     */
    public IMetadataProvider getMedatadaProvider(){

        return new ElasticsearchMetaProvider(connectionHandle);
    }

    /**
     * Return the QueryEngine.
     *
     * @return the QueryEngine
     */
    @Override
    public IQueryEngine getQueryEngine(){

        return new ElasticsearchQueryEngine(connectionHandle);
    }

    /**
     * Return the MetadataEngine.
     *
     * @return the MetadataEngine
     */
    @Override
    public IMetadataEngine getMetadataEngine()  {
        return  new ElasticsearchMetadataEngine(connectionHandle);
    }



    /**
	 * Create a connection to Elasticsearch. 
	 * The client will be a transportClient by default unless stratio nodeClient is specified.
	 * 
	 * @param configuration   the connection configuration. It must be not null.
	 *
	 * @throws InitializationException        on failure connection.
	 */
	
	@Override
	public void init(IConfiguration configuration) throws InitializationException {

         connectionHandle = new ConnectionHandle(configuration);


	}

    @Override
    public void connect(ICredentials credentials, ConnectorClusterConfig config) throws ConnectionException {
        //TODO al implementar la nueva interfaz

        connectionHandle.createConnection(credentials,config);
    }


    /**
	 * Close the Mongo's connection.
	 *
     * @param name
     */
	@Override
	public void close(ClusterName name) {
        connectionHandle.closeConnection(name.getName());

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
	* The connection status.
	*
	* @return true if the driver's client is not null.
     * @param name the cluster Name.
	*/
	@Override
	public boolean isConnected(ClusterName name) {

		return connectionHandle.isConnected(name.getName());

	}


//	 private void createNodeClient(ElasticsearchClientConfiguration elasticConfiguration, ICredentials credentials) throws InitializationException {
//		 if(elasticClient == null){
//			// TODO settings are overwritten?
//			 if (credentials == null) {
//				 NodeBuilder nodeBuilder = nodeBuilder();
//				 Node node = nodeBuilder.settings(elasticConfiguration.getSettings()).node();
//				 elasticClient = node.client();
//				 logger.info("Elasticsearch connection established ");
//			 }else throw new InitializationException("Credentials are not supported");
//		 }
//	}
//
//
//
//		private void createTransportClient(ElasticsearchClientConfiguration elasticConfiguration,
//				ICredentials credentials) throws InitializationException {
//
//			if (elasticClient == null) {
//
//				if (credentials == null) {
//					elasticClient = new TransportClient(elasticConfiguration.getSettings())
//					.addTransportAddresses(getTransportAddressFromString(elasticConfiguration.getSeeds()));
//					logger.info("Elasticsearch connection established ");
//				} else throw new InitializationException("Credentials are not supported");
//
//			}
//		} //REVIEW por cambio de arquitectura
		

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
