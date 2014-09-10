package com.stratio.connector.elasticsearch.core.engine; 

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.elasticsearch.core.connection.ElasticSearchConnectionHandler;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.IndexName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.ColumnMetadata;
import com.stratio.meta2.common.metadata.IndexMetadata;
import com.stratio.meta2.common.metadata.TableMetadata;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;


import static org.hamcrest.CoreMatchers.is;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;



/** 
* ElasticsearchStorageEngine Tester. 
* 
* @author <Authors name> 
* @since <pre>sep 10, 2014</pre> 
* @version 1.0 
*/
@RunWith(PowerMockRunner.class)
public class ElasticsearchStorageEngineTest {

    public static final Integer INTEGER_CELL_VALUE = new Integer(5);
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private static final String CLUSTER_NAME = "CLUSTER NAME";
    private static final String INDEX_NAME = "INDEX_NAME";
    private static final String TYPE_NAME = "TYPE_NAME";
    public static final String ROW_NAME = "row_name";
    public static final String CELL_VALUE = "cell_value";
    private static final String OTHER_ROW_NAME = "OTHER_ROW_NAME" ;


    ElasticsearchStorageEngine elasticsearchStorageEngine;



    private TableName table_name = new TableName(INDEX_NAME, TYPE_NAME);
    private Map<ColumnName, ColumnMetadata> columns = null;
    private Map<String, Object> options = null;
    private Map<IndexName, IndexMetadata> indexes = null;
    private ClusterName clusterRef = null;
    private List<ColumnName> partirionKey = Collections.emptyList();
    private List<ColumnName> clusterKey  = Collections.emptyList();

    @Mock private ElasticSearchConnectionHandler connectionHandler;
    @Mock private Connection<Client> connection;
    @Mock private Client client;


    @Before
    public void before() throws HandlerConnectionException {
        when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection);
        when(connection.getNativeConnection()).thenReturn(client);
        elasticsearchStorageEngine = new ElasticsearchStorageEngine(connectionHandler);
    }

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: insert(ClusterName targetCluster, TableMetadata targetTable, Row row) 
* 
*/ 
@Test
public void testInsertOne() throws UnsupportedException, ExecutionException {

    ClusterName clusterName = new ClusterName(CLUSTER_NAME);



    TableMetadata targetTable = new TableMetadata(table_name,options,columns,indexes,clusterRef, partirionKey,clusterKey);
    Row row = createRow(ROW_NAME, CELL_VALUE);



    IndexRequestBuilder indexRequestBuilder = mock(IndexRequestBuilder.class);
    Map<String, Object> map = new HashMap<>();
    map.put(ROW_NAME,CELL_VALUE);
    when(indexRequestBuilder.setSource(eq(map))).thenReturn(indexRequestBuilder);
    when(client.prepareIndex(INDEX_NAME, TYPE_NAME)).thenReturn(indexRequestBuilder);
    ListenableActionFuture<IndexResponse> listenableActionFuture = mock(ListenableActionFuture.class);
    when(indexRequestBuilder.execute()).thenReturn(listenableActionFuture);


    elasticsearchStorageEngine.insert(clusterName, targetTable, row);


    verify(listenableActionFuture,times(1)).actionGet();

}



    @Test
    public void testInsertOnePK() throws UnsupportedException, ExecutionException {

        ClusterName clusterName = new ClusterName(CLUSTER_NAME);

        partirionKey = new ArrayList<>();
        partirionKey.add(new ColumnName(INDEX_NAME,TYPE_NAME, ROW_NAME));

        TableMetadata targetTable = new TableMetadata(table_name,options,columns,indexes,clusterRef, partirionKey,clusterKey);
        Row row = createRow(ROW_NAME, CELL_VALUE);



        IndexRequestBuilder indexRequestBuilder = mock(IndexRequestBuilder.class);
        Map<String, Object> map = new HashMap<>();
        map.put(ROW_NAME,CELL_VALUE);
        when(indexRequestBuilder.setSource(eq(map))).thenReturn(indexRequestBuilder);
        when(client.prepareIndex(INDEX_NAME, TYPE_NAME, CELL_VALUE)).thenReturn(indexRequestBuilder);
        ListenableActionFuture<IndexResponse> listenableActionFuture = mock(ListenableActionFuture.class);
        when(indexRequestBuilder.execute()).thenReturn(listenableActionFuture);


        elasticsearchStorageEngine.insert(clusterName, targetTable, row);


        verify(listenableActionFuture,times(1)).actionGet();

    }


    @Test
    public void testExceptionInsertTwoPK() throws UnsupportedException, ExecutionException {

        exception.expect(UnsupportedException.class);
        exception.expectMessage("Only one PK is allowed");

        ClusterName clusterName = new ClusterName(CLUSTER_NAME);

        partirionKey = new ArrayList<>();
        partirionKey.add(new ColumnName(INDEX_NAME,TYPE_NAME, ROW_NAME));
        partirionKey.add(new ColumnName(INDEX_NAME,TYPE_NAME, OTHER_ROW_NAME));

        TableMetadata targetTable = new TableMetadata(table_name,options,columns,indexes,clusterRef, partirionKey,clusterKey);
        Row row = createRow(ROW_NAME, CELL_VALUE);
        row.addCell(OTHER_ROW_NAME,new Cell(CELL_VALUE));



        IndexRequestBuilder indexRequestBuilder = mock(IndexRequestBuilder.class);
        Map<String, Object> map = new HashMap<>();
        map.put(ROW_NAME,CELL_VALUE);


        elasticsearchStorageEngine.insert(clusterName, targetTable, row);

    }

    @Test
    public void testExceptionInsertIntegerPK() throws UnsupportedException, ExecutionException {

        exception.expect(UnsupportedException.class);
        exception.expectMessage("The PK only can has String values");

        ClusterName clusterName = new ClusterName(CLUSTER_NAME);

        partirionKey = new ArrayList<>();
        partirionKey.add(new ColumnName(INDEX_NAME,TYPE_NAME, ROW_NAME));


        TableMetadata targetTable = new TableMetadata(table_name,options,columns,indexes,clusterRef, partirionKey,clusterKey);
        Row row = createRow(ROW_NAME, INTEGER_CELL_VALUE);


        IndexRequestBuilder indexRequestBuilder = mock(IndexRequestBuilder.class);
        Map<String, Object> map = new HashMap<>();
        map.put(ROW_NAME,INTEGER_CELL_VALUE);



        elasticsearchStorageEngine.insert(clusterName, targetTable, row);

    }


    @Test
    public void testInsertExecutionException() throws HandlerConnectionException, UnsupportedException, ExecutionException {

       exception.expect(ExecutionException.class);
        exception.expectMessage("Fail Connecting elasticSearch. Msg");



        when(connectionHandler.getConnection(CLUSTER_NAME)).thenThrow(new HandlerConnectionException("Msg"));

        TableMetadata targetTable = new TableMetadata(table_name,options,columns,indexes,clusterRef, partirionKey,clusterKey);
        ClusterName clusterName = new ClusterName(CLUSTER_NAME);


           elasticsearchStorageEngine.insert(clusterName, targetTable, createRow(ROW_NAME, INTEGER_CELL_VALUE));


    }
    private Row createRow(String rowKey, Object cellValue) {
        Cell cell = new Cell(cellValue);
        Row row = new Row(rowKey,cell);
        return row;
    }

    /**
* 
* Method: insert(ClusterName targetCluster, TableMetadata targetTable, Collection<Row> rows) 
* 
*/ 
@Test
public void testInsertForTargetClusterTargetTableRows() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: insert(Client elasticClient, String index, String type, Row row, String id) 
* 
*/ 
@Test
public void testInsertForElasticClientIndexTypeRowId() throws Exception { 
//TODO: Test goes here... 
} 


/** 
* 
* Method: insert(Client client, TableMetadata targetTable, Row row) 
* 
*/ 
@Test
public void testInsertForClientTargetTableRow() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = ElasticsearchStorageEngine.getClass().getMethod("insert", Client.class, TableMetadata.class, Row.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/ 
} 

/** 
* 
* Method: insert(Client elasticClient, TableMetadata targetTable, Collection<Row> rows) 
* 
*/ 
@Test
public void testInsertForElasticClientTargetTableRows() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = ElasticsearchStorageEngine.getClass().getMethod("insert", Client.class, TableMetadata.class, Collection<Row>.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/ 
} 

/** 
* 
* Method: createIndexRequestBuilder(TableMetadata targetTable, Client elasticClient, String index, String type, Row row) 
* 
*/ 
@Test
public void testCreateIndexRequestBuilderForTargetTableElasticClientIndexTypeRow() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = ElasticsearchStorageEngine.getClass().getMethod("createIndexRequestBuilder", TableMetadata.class, Client.class, String.class, String.class, Row.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/ 
} 

/** 
* 
* Method: delete(Client elasticClient, String index, String type, Filter... filters) 
* 
*/ 
@Test
public void testDelete() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = ElasticsearchStorageEngine.getClass().getMethod("delete", Client.class, String.class, String.class, Filter....class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/ 
} 

/** 
* 
* Method: createIndexRequestBuilder(Client elasticClient, String index, String type, Row row, String id) 
* 
*/ 
@Test
public void testCreateIndexRequestBuilderForElasticClientIndexTypeRowId() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = ElasticsearchStorageEngine.getClass().getMethod("createIndexRequestBuilder", Client.class, String.class, String.class, Row.class, String.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/ 
} 

/** 
* 
* Method: validateDelete(DeleteByQueryResponse response) 
* 
*/ 
@Test
public void testValidateDelete() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = ElasticsearchStorageEngine.getClass().getMethod("validateDelete", DeleteByQueryResponse.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/ 
} 

/** 
* 
* Method: isEmpty(String value) 
* 
*/ 
@Test
public void testIsEmpty() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = ElasticsearchStorageEngine.getClass().getMethod("isEmpty", String.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/ 
} 

} 
