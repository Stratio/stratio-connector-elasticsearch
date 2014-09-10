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
package com.stratio.connector.elasticsearch.core.engine;



import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.meta.common.connector.IMetadataEngine;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.CatalogMetadata;
import com.stratio.meta2.common.metadata.TableMetadata;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.client.Client;

/**
 *
 * This class is the responsible of manage the ElasticSearchMetadata
 * @author darroyo
 *
 */
public class ElasticsearchMetadataEngine implements IMetadataEngine{

    /**
     * The connection handle.
     */
	private transient ConnectionHandler connectionHandler;

    /**
     * Constructor.
     * @param  connectionHandler the connector handle.
     */
    public ElasticsearchMetadataEngine(ConnectionHandler connectionHandler) {
        this.connectionHandler = connectionHandler;
    }


    /**
     * This method create a index in ES.
     * @param  targetCluster the cluster to be created.
     * @param  indexMetaData the index configuration.
     */

    @Override
    public void createCatalog(ClusterName targetCluster, CatalogMetadata indexMetaData) throws UnsupportedException  {
        throw new UnsupportedException("Not yet supported");
    }
    /**
     * This method create a type in ES.
     * @param  targetCluster the cluster to be created.
     * @param  typeMetaData the type configuration.
     */

    @Override
    public void createTable(ClusterName targetCluster, TableMetadata typeMetaData) throws UnsupportedException  {
        throw new UnsupportedException("Not yet supported");
    }

    /**
     * This method drop a index in ES.
     * @param  targetCluster the cluster to be created.
     * @param  indexName the index name.
     */

    @Override
    public void dropCatalog(ClusterName targetCluster, CatalogName indexName) throws ExecutionException {
        DeleteIndexResponse delete = null;
        try {
            delete = recoveredClient(targetCluster).admin().indices().delete(new DeleteIndexRequest(indexName.getName())).actionGet();
            if (!delete.isAcknowledged()) throw new ExecutionException("dropCatalog request has not been acknowledged");
        } catch (HandlerConnectionException e) {
            e.printStackTrace();
        }


    }

    /**
     * This method drop a type in ES.
     * @param  targetCluster the cluster to be created.
     * @param  typeName the type name.
     */
    @Override
    public void dropTable(ClusterName targetCluster, TableName typeName) throws  ExecutionException {
        DeleteMappingResponse delete = null;
        try {
            delete = recoveredClient(targetCluster).admin().indices().prepareDeleteMapping(typeName.getCatalogName().getName()).setType(typeName.getName()).execute().actionGet();
            if (!delete.isAcknowledged()) throw new ExecutionException("dropTable request has not been acknowledged");
        } catch (HandlerConnectionException e) {
            e.printStackTrace(); //TODO
        }



    }


    /**
     * This method return the concrete ES Client for a cluster,
     * @param targetCluster the cluster identification.
     * @return the ES Client.
     */
    private Client recoveredClient(ClusterName targetCluster) throws HandlerConnectionException {
        return (Client) connectionHandler.getConnection(targetCluster.getName()).getNativeConnection();
    }
}

