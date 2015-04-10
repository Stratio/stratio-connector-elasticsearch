/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.elasticsearch.core.engine;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.engine.query.ProjectParsed;
import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryBuilder;
import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryExecutor;
import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryParser;
import com.stratio.connector.elasticsearch.core.engine.query.ESProjectParsedValidator;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.result.QueryResult;

/**
 * ElasticsearchQueryEngine Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>oct 15, 2014</pre>
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ ElasticsearchQueryEngine.class, ConnectorQueryParser.class, ConnectorQueryBuilder.class,
        ConnectorQueryExecutor.class })
public class ElasticsearchQueryEngineTest {

    @Mock ConnectionHandler connectionHandle;
    @Mock private ConnectorQueryParser queryParser;
    @Mock private ConnectorQueryBuilder queryBuilder;
    @Mock private ConnectorQueryExecutor queryExecutor;
    private ElasticsearchQueryEngine elasticsearchQueryEngine;

    @Before
    public void before() throws Exception {

        whenNew(ConnectorQueryParser.class).withNoArguments().thenReturn(queryParser);
        whenNew(ConnectorQueryBuilder.class).withNoArguments().thenReturn(queryBuilder);
        whenNew(ConnectorQueryExecutor.class).withNoArguments().thenReturn(queryExecutor);

        elasticsearchQueryEngine = new ElasticsearchQueryEngine(connectionHandle);

    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: execute(Project logicalWorkflow, Connection<Client> connection)
     */
    @Test
    public void testExecute() throws Exception {

        Project project = mock(Project.class);
        Connection<Client> connection = mock(Connection.class);
        Client eSConnection = mock(Client.class);
        when(connection.getNativeConnection()).thenReturn(eSConnection);

        ProjectParsed projectParsed = mock(ProjectParsed.class);
        ESProjectParsedValidator projectValidator = mock(ESProjectParsedValidator.class);
        whenNew(ESProjectParsedValidator.class).withNoArguments().thenReturn(projectValidator);
        whenNew(ProjectParsed.class).withArguments(project, projectValidator).thenReturn(projectParsed);



        SearchRequestBuilder searchRequestBuilder = mock(SearchRequestBuilder.class);
        when(queryBuilder.buildQuery(eSConnection, projectParsed)).thenReturn(searchRequestBuilder);

        ResultSet resultSet = mock(ResultSet.class);

        QueryResult queryResult = QueryResult.createQueryResult(resultSet, 0, true);
        when(queryExecutor.executeQuery(eSConnection, searchRequestBuilder, projectParsed)).thenReturn(queryResult);

        QueryResult returnQueryResult = elasticsearchQueryEngine.execute(project, connection);
        assertEquals("The query result is correct", queryResult, returnQueryResult);

    }

    /**
     * Method: asyncExecute(String queryId, LogicalWorkflow workflow, IResultHandler resultHandler)
     */
    @Test(expected = UnsupportedException.class)
    public void testAsyncExecute() throws ConnectorException {
        elasticsearchQueryEngine.asyncExecute("", mock(LogicalWorkflow.class), mock(IResultHandler.class));
    }

    /**
     * Method: stop(String queryId)
     */
    @Test(expected = UnsupportedException.class)
    public void testStop() throws UnsupportedException, ExecutionException {
        elasticsearchQueryEngine.stop("");
    }

} 
