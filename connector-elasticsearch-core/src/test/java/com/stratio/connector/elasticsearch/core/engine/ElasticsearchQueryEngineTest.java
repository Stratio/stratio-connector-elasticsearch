package com.stratio.connector.elasticsearch.core.engine; 

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryBuilder;
import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryData;
import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryExecutor;
import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryParser;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.result.QueryResult;

/** 
* ElasticsearchQueryEngine Tester. 
* 
* @author <Authors name> 
* @since <pre>oct 15, 2014</pre> 
* @version 1.0 
*/
@RunWith(PowerMockRunner.class)
@PrepareForTest({ElasticsearchQueryEngine.class,ConnectorQueryParser.class,ConnectorQueryBuilder.class,
        ConnectorQueryExecutor.class})
public class ElasticsearchQueryEngineTest {

    @Mock private ConnectorQueryParser queryParser;
    @Mock private ConnectorQueryBuilder queryBuilder;
    @Mock private ConnectorQueryExecutor queryExecutor;


   private ElasticsearchQueryEngine elasticsearchQueryEngine;

    @Mock ConnectionHandler connectionHandle;
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
* 
* Method: execute(Project logicalWorkflow, Connection<Client> connection) 
* 
*/ 
@Test
public void testExecute() throws Exception {

    Project project = mock(Project.class);
    Connection<Client> connection = mock(Connection.class);
    Client eSConnection = mock (Client.class);
    when(connection.getNativeConnection()).thenReturn(eSConnection);

    ConnectorQueryData conectorQueryData = mock (ConnectorQueryData.class);

    when(queryParser.transformLogicalWorkFlow(project)).thenReturn(conectorQueryData);
    SearchRequestBuilder searchRequestBuilder = mock (SearchRequestBuilder.class);
    when(queryBuilder.buildQuery(eSConnection, conectorQueryData)).thenReturn(searchRequestBuilder);
    QueryResult queryResult = mock(QueryResult.class);
    when(queryExecutor.executeQuery(eSConnection, searchRequestBuilder, conectorQueryData)).thenReturn(queryResult);


    QueryResult returnQueryResult =  elasticsearchQueryEngine.execute(project, connection);
    assertEquals("The query result is correct",queryResult,returnQueryResult);



} 

/** 
* 
* Method: asyncExecute(String queryId, LogicalWorkflow workflow, IResultHandler resultHandler) 
* 
*/ 
@Test(expected = UnsupportedException.class)
public void testAsyncExecute() throws UnsupportedException, ExecutionException {
    elasticsearchQueryEngine.asyncExecute("",mock(LogicalWorkflow.class),mock(IResultHandler.class));
} 

/** 
* 
* Method: stop(String queryId) 
* 
*/ 
@Test(expected = UnsupportedException.class)
public void testStop() throws UnsupportedException, ExecutionException {
    elasticsearchQueryEngine.stop("");
} 


} 
