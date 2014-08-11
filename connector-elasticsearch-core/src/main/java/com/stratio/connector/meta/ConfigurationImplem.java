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

package com.stratio.connector.meta;

import com.stratio.meta.common.connector.IConfiguration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jmgomez on 14/07/14.
 */
public class ConfigurationImplem implements IConfiguration{

    private static Map<String,String> config;
    
    static{
    			/*
		    	public static final int SCAN_TIMEOUT_MILLIS = 600000;
		    	public static final int SIZE_QUERY_ANDTHEN_FETCH = 10;
		    	public static final int SIZE_SCAN = 10;
    			 */
    	
    	        config = new HashMap<String, String>();
    	        //STRATIO_CONNECTOR
    	        //Connector client acts as a node within the cluster (with benefits). Otherwise, client connect remotely to the ES cluster
    	        config.put("elasticsearch.stratio.nodeclient", "false");
    	        
    	        
    	        //NODE => Development configuration
    	        config.put("elasticsearch.node.data", "false");
    	        config.put("elasticsearch.node.master", "false");
    	        
    	        
    	        
    	        //PATHS => NO
    	        //PLUGIN =>NO
    	        
    	        //CLIENT
    	        config.put("elasticsearch.client.transport.sniff","true");//The client allows to sniff the rest of the cluster, and add those into its list of machines to use
    	        //config.put("client.transport.ignore_cluster_name", "true");
    	        //client.transport.ping_timeout 5s
    	        //client.transport.nodes_sampler_interval 5s
    	        
    	        //CLUSTER
    	        config.put("elasticsearch.cluster.name", "david_cluster");
    	        //add as many as you want
    	        

    	        //NETWORK
				//config.put("network.host", "127.0.0.1");
					
    	        //GATEWAY
				//config.put("gateway.type", "none");
    	        
    	        //INDEX (foreach index?)
//    	        config.put("index.store.type", "memory")
//				config.put("index.number_of_shards", 1)
//				config.put("index.number_of_replicas", 1)
    	        
    	        //timeout en delete, search, etc..
    	        //acknowledge => shards, shards and replicas, etc...
    	        
    	        //insert => setWriteConsistencyLevel (ALL, DEFAULT, ONE, QUORUM)
    	        //en Cassandra: QUORUM 	A write must be written to the commit log and memory table on a quorum of replica nodes.
    	        //ONE 	A write must be written to the commit log and memory table of at least one replica node.
    	        //ALL 	A write must be written to the commit log and memory table on all replica nodes in the cluster for that row key.
    	        //DEFAULT => QUORUM (>rep/2 +1 ) to prevent writes in a wrong side of a network partition.
    	        //action.write_consistency, all, quorum etc...
    	        //refresh a true after insert a doc
    	        
    	        //insert=>replicationType => SYNC(default), ASYNC
    	        //index with ttl => disabled by default
    	        
    	        //opciones de index with update
    	        //http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/indices-update-settings.html
    	        
    	        //opciones de cluster
    	        //http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/cluster-update-settings.html
    	        
    	        //distintos módulo
    	        //http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/modules.html
    }
 
    private static List<String> seeds;
    
    static{
    	seeds = Arrays.asList("localhost:9300");
    	//seeds = Arrays.asList("172.19.0.77:9300");
    }
    
    //Credentials...
    
    public String getProperty(String key){

        return config.get(key);
    }
    
    public Map<String,String> getMapProperties(){
    	return config;
    }

    public boolean exist(String key){
                return config.containsKey(key);
    }
    
    public List<String> getSeeds(){
		return seeds;
    }
    
    
}
