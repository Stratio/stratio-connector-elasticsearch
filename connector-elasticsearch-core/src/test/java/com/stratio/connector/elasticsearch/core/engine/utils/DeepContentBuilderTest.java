package com.stratio.connector.elasticsearch.core.engine.utils; 

import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.ColumnMetadata;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.TableMetadata;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/** 
* DeepContentBuilder Tester. 
* 
* @author <Authors name> 
* @since <pre>sep 11, 2014</pre> 
* @version 1.0 
*/ 
public class DeepContentBuilderTest {


    public static final String INDEX_NAME = "index";
    public static final String TYPE_NAME = "type";
    public static final String CLUSTER_NAME = "CLUSTER_NAME";
    DeepContentBuilder deepContentBuilder;

    private static final String RESULT_CREATE_TABLE ="{\"properties\":{\"column_7\":{\"type\":\"string\"},\"column_5\":{\"type\":\"integer\"},\"column_1\":{\"type\":\"long\"},\"column_3\":{\"type\":\"double\"},\"column_2\":{\"type\":\"boolean\"},\"column_4\":{\"type\":\"float\"},\"column_6\":{\"type\":\"string\"}}}";
@Before
public void before() throws Exception {

    deepContentBuilder = new DeepContentBuilder();
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: createTypeSource(TableMetadata typeMetadata)
* 
*/ 
@Test
public void testCreateTable() throws Exception {

    Map indexex =  Collections.EMPTY_MAP;
    Map<String, Object> options = Collections.EMPTY_MAP;
    List<ColumnName> partitionKey = Collections.EMPTY_LIST;
    List<ColumnName> clusterKey  = Collections.EMPTY_LIST;


    Map<ColumnName,ColumnMetadata> columns= new HashMap<>();
    columns.putAll(creteColumns("column_1",ColumnType.BIGINT));
    columns.putAll(creteColumns("column_2",ColumnType.BOOLEAN));
    columns.putAll(creteColumns("column_3",ColumnType.DOUBLE));
    columns.putAll(creteColumns("column_4",ColumnType.FLOAT));
    columns.putAll(creteColumns("column_5",ColumnType.INT));
    columns.putAll(creteColumns("column_6",ColumnType.TEXT));
    columns.putAll(creteColumns("column_7",ColumnType.VARCHAR));

    ClusterName cluteref = new ClusterName(CLUSTER_NAME);

    TableMetadata tableMetadata = new TableMetadata(new TableName(INDEX_NAME,TYPE_NAME),options, columns,indexex,cluteref,partitionKey,clusterKey);
    assertEquals("The JSON is correct",RESULT_CREATE_TABLE,(deepContentBuilder.createTypeSource(tableMetadata).string()));

}


    private Map<ColumnName,ColumnMetadata> creteColumns(String columnName, ColumnType columnType){
        Map<ColumnName,ColumnMetadata> columns= new HashMap<>();
        ColumnName column = new ColumnName(INDEX_NAME, TYPE_NAME, columnName);
          columns.put(column, new ColumnMetadata(column,null, columnType));

        return columns;
    }




} 
