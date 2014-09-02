package com.stratio.connector.elasticsearch.core.connection;

import com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.security.ICredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jmgomez on 28/08/14.
 */
public class ConnectionHandle {

    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());
    /**
     * The general settings.
     */
    private IConfiguration configuration;

    /**
     * The connections.
     */
    private Map<String,Connection> connections = new HashMap<>();

    /**
     * Constructor.
     * @param configuration the general settings.
     */
    public ConnectionHandle(IConfiguration configuration) {
        this.configuration = configuration;

    }

    /**
     * This method create a connection.
     * @param credentials the cluster configuration.
     * @param config the connection options.
     */
    public void createConnection(ICredentials credentials, ConnectorClusterConfig config) {
        Connection connection = createConcreteConnection(credentials, config);

        connections.put(config.getName().getName(), connection);

        logger.info("Create a ElasticSearch connection [clusterName]");
        //TODO que hacer si ya existe la conexion.

    }

    private Connection createConcreteConnection(ICredentials credentials, ConnectorClusterConfig config) {
        Connection connection;
        if (isNodeClient(config)) {
            connection =new NodeConnection(credentials,config);
        }else{
            connection = new TransportConnection(credentials,config);
        }
        return connection;
    }

    public void closeConnection(String clusterName) {
        if (connections.containsKey(clusterName)){
            connections.get(clusterName).close();
            connections.remove(clusterName);
            logger.info("Disconnected from Elasticsearch ["+clusterName+"]");
        }
    }

    public boolean isConnected(String clusterName) {
        boolean isConnected = false;
        if (connections.containsKey(clusterName)){
            isConnected =  connections.get(clusterName).isConnect();
        }
        return  isConnected;
    }

    private boolean isNodeClient(ConnectorClusterConfig config) {
        return Boolean.parseBoolean((String)config.getOptions().get(ConfigurationOptions.NODE_TYPE.getOptionName()));
    }


    public Connection getConnection(String name) {
        Connection connection = null;
        if (connections.containsKey(name)){
            connection = connections.get(name);
        }else{
            //REVIEW lanzar excepcion
        }
        return connection;
    }
}
