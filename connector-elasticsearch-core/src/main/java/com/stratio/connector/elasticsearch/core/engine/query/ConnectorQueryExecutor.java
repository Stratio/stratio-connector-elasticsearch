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

package com.stratio.connector.elasticsearch.core.engine.query;

import com.stratio.connector.elasticsearch.core.engine.utils.LimitModifier;
import com.stratio.connector.meta.ElasticsearchResultSet;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.QualifiedNames;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


/**
 * Created by jmgomez on 15/09/14.
 */
public class ConnectorQueryExecutor {

    /**
     * The log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * This method execute a query in elasticSearch.
     *
     * @param elasticClient  the elasticSearch Client.
     * @param requestBuilder the query to execute.
     * @param queryData      the queryData.
     * @return the query result.
     */

    public QueryResult executeQuery(Client elasticClient, SearchRequestBuilder requestBuilder, ConnectorQueryData queryData) {

        SearchType searchType = queryData.getSearchType();

        QueryResult queryResult = null;
        try {

            ElasticsearchResultSet resultSet = new ElasticsearchResultSet();
            SearchResponse scrollResp = requestBuilder.execute().actionGet();


            do {
                if (searchType == SearchType.SCAN) {
                    scrollResp = elasticClient.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(LimitModifier.SCAN_TIMEOUT_MILLIS)).execute().actionGet();
                }

                for (SearchHit hit : scrollResp.getHits()) {
                    resultSet.add(createRow(hit,queryData));

                }

            } while (scrollResp.getHits().getHits().length != 0);


            queryResult = QueryResult.createQueryResult(resultSet);
        } catch (IndexMissingException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("The index not exists. The ES connector returns an empty QueryResult. "+e.getMessage());
            }
            queryResult = QueryResult.createQueryResult(new ElasticsearchResultSet());
        }
        return queryResult;
    }

    /**
     * This method creates a row from a mongoResult
     *
     * @param hit the Elasticsearch SearchHit.
     * @param queryData
     * @return the row.
     */
    private Row createRow(SearchHit hit, ConnectorQueryData queryData) {
        Row row = new Row();
           Map<String, String> alias = queryData.getAlias();
        for (Map.Entry<String, SearchHitField> entry : hit.fields().entrySet()) {

            String key = entry.getKey();
            Project projection = queryData.getProjection();
            String qualifiedFieldName = QualifiedNames.getColumnQualifiedName(projection.getCatalogName(),projection.getTableName().getName(),key);

            if (alias.containsKey(qualifiedFieldName)){
                key = alias.get(qualifiedFieldName);
            }

            row.addCell(key, new Cell(entry.getValue().getValue()));
        }

        return row;
    }


}
