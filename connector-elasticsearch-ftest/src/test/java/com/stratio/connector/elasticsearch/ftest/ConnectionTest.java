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

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.stratio.connector.elasticsearch.core.ElasticsearchConnector;
import com.stratio.connector.meta.ConfigurationImplem;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.InitializationException;


public class ConnectionTest {

    protected final String COLLECTION = getClass().getSimpleName();
    protected final String CATALOG = "functionaltest";
    protected final Long MILLIS = 1200l;
    
    
    /**
     * The elasticsearch elasticClient.
     */
    protected Client client = null;
    protected ElasticsearchConnector stratioElasticConnector = null;
//    MongoClientOptions cilentOptions = null;
    protected String clusterName = "david_cluster";
    protected String IP = "localhost";//"172.19.0.77"
    
    @Before
    public void setUp() throws InitializationException {
//        try {
           
            stratioElasticConnector = new ElasticsearchConnector();
            stratioElasticConnector.init(null, new ConfigurationImplem());
//            MongoClientConfiguration confi = (MongoClientConfiguration) stratioMongoConnector.getConfiguration();
//            MongoClientOptions options =  confi.getMongoClientOptions();
//            
            
            InetSocketTransportAddress[] seeds = new InetSocketTransportAddress[1];
			InetSocketTransportAddress ta = new InetSocketTransportAddress(IP, 9300);
			seeds[0] = ta;
			
			Settings settings = ImmutableSettings.settingsBuilder()
			        .put("cluster.name",clusterName).build();

			client = new TransportClient(settings)
			.addTransportAddresses(seeds);
			
			
//            ArrayList<ServerAddress> seeds = new ArrayList<ServerAddress>(3);;
//            
//            for(String server: confi.getSeeds()){
//				String[] array =  server.split(":");
//				try{
//					if(array.length != 2) throw new InitializationException("invalid address => host:port");
//					else seeds.add(new ServerAddress(array[0],Integer.decode(array[1])));
//					
//				}catch(UnknownHostException e){
//					e.printStackTrace();
//					throw new InitializationException("Connection failed");
//				}
//			}
//            
//            mongoClient = new MongoClient(seeds);

//        } catch (MongoException | InitializationException e) {
//            e.printStackTrace();
//        }
    }

    private void deleteSet(){
    	deleteSet(CATALOG);
    }
    
    
    private void deleteSet(String catalog){
    	try {
    		client.admin().indices().delete(new DeleteIndexRequest(catalog)).actionGet();
		} catch (Exception e) {
			// TODO: handle exception
		}
    }
    

    protected void refresh(){
    	refresh(CATALOG);
    }
    protected void refresh(String catalog){
    	try {
    	client.admin().indices().flush(new FlushRequest(catalog)).actionGet();
    	} catch (Exception e) {
			// TODO: handle exception
		}
    }
    
    @After
    public void tearDown() throws ConnectionException {
    	deleteSet();
        client.close();
        stratioElasticConnector.close();
    }

}
