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

import com.stratio.connector.elasticsearch.core.connection.ElasticSearchConnectionHandle;
import com.stratio.connector.meta.IMetadataProvider;
import com.stratio.connector.meta.exception.UnsupportedOperationException;
import com.stratio.meta.common.exceptions.ExecutionException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.client.Client;

/**
 * This class represents a MetaInfo Provider for Elasticsearch.
 */

public class ElasticsearchMetaProvider implements IMetadataProvider {
    /**
     * The connection.
     */
    private Client elasticClient = null;

    private transient ElasticSearchConnectionHandle connectionHandle;

    public ElasticsearchMetaProvider(ElasticSearchConnectionHandle connectionHandle) {

        this.connectionHandle = connectionHandle;
    }


    @Override
    public void createCatalog(String catalog) throws UnsupportedOperationException {
        //TODO index settings?
        throw new UnsupportedOperationException("Not yet supported");
    }

    @Override
    public void createTable(String catalog, String table) throws UnsupportedOperationException {
        //TODO type mappings?
        throw new UnsupportedOperationException("Not yet supported");
    }

    @Override
    public void dropCatalog(String catalog) throws UnsupportedOperationException, ExecutionException {
        DeleteIndexResponse delete = elasticClient.admin().indices().delete(new DeleteIndexRequest(catalog)).actionGet();
        if (!delete.isAcknowledged()) throw new ExecutionException("dropCatalog request has not been acknowledged");

    }


    @Override
    public void dropTable(String catalog, String table) throws UnsupportedOperationException, ExecutionException {
        //drop mapping => the table will be deleted
        DeleteMappingResponse delete = elasticClient.admin().indices().prepareDeleteMapping(catalog).setType(table).execute().actionGet();

        if (!delete.isAcknowledged()) throw new ExecutionException("dropCatalog request has not been acknowledged");
        //TODO configure level??

    }


    @Override
    public void createIndex(String catalog, String tableName, String... fields) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Not supported");

    }

    @Override
    public void dropIndex(String catalog, String tableName, String... fields) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("not supported");

    }

    @Override
    public void dropIndexes(String catalog, String tableName) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("not supported");

    }


}
