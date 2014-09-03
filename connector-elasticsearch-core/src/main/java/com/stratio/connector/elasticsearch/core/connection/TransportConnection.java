package com.stratio.connector.elasticsearch.core.connection;

import com.stratio.connector.elasticsearch.core.configuration.ElasticsearchClientConfiguration;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.security.ICredentials;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * This class represents a logic connection.
 * Created by jmgomez on 28/08/14.
 */
public class TransportConnection extends Connection<Client> {


    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    ElasticsearchClientConfiguration elasticsearchClientConfiguration = new ElasticsearchClientConfiguration();

    /**
     * The Elasticsearch client.
     */
    private Client elasticClient = null;  //REVIEW posiblemente esta clase desaparezca ya que la conexion no esta aqui.



    /**
     * Constructor.
     * @param credentiasl the credentials.
     *
     * @param config The cluster configuration.
     */
    public TransportConnection(ICredentials credentiasl, ConnectorClusterConfig config){


               elasticClient = new TransportClient(ElasticsearchClientConfiguration.getSettings(config))
                        .addTransportAddresses(elasticsearchClientConfiguration.getTransporAddress(config));
                logger.info("Elasticsearch Transport connection established ");


            isConnect = true;
        }








    public void close() {
        if (elasticClient != null) {
            elasticClient.close();
            isConnect=false;
            elasticClient = null;

        }

    }

    @Override
    public Client getClient() {
        return elasticClient;
    }

}
