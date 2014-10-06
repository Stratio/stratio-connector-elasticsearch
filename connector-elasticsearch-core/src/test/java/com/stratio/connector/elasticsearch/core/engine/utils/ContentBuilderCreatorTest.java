package com.stratio.connector.elasticsearch.core.engine.utils;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.ColumnMetadata;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.TableMetadata;
import com.stratio.meta2.common.statements.structures.selectors.Selector;

/**
 * DeepContentBuilder Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>sep 11, 2014</pre>
 */
public class ContentBuilderCreatorTest {

    public static final String INDEX_NAME = "index";
    public static final String TYPE_NAME = "type";
    public static final String CLUSTER_NAME = "CLUSTER_NAME";
    private static final String RESULT_CREATE_TABLE = "{\"properties\":{\"column_3\":{\"type\":\"double\"}," +
            "\"column_5\":{\"type\":\"integer\"},\"column_7\":{\"type\":\"string\"}," +
            "\"column_2\":{\"type\":\"boolean\"},\"column_6\":{\"type\":\"string\"},\"column_4\":{\"type\":\"float\"},\"column_1\":{\"type\":\"long\"}}}";
    ContentBuilderCreator deepContentBuilder;

    @Before
    public void before() throws Exception {

        deepContentBuilder = new ContentBuilderCreator();
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: createTypeSource(TableMetadata typeMetadata)
     */
    @Test
    public void testCreateTable() throws Exception {

        Map indexex = Collections.EMPTY_MAP;
        Map<Selector, Selector> options = Collections.EMPTY_MAP;
        List<ColumnName> partitionKey = Collections.EMPTY_LIST;
        List<ColumnName> clusterKey = Collections.EMPTY_LIST;

        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        columns.putAll(creteColumns("column_1", ColumnType.BIGINT));
        columns.putAll(creteColumns("column_2", ColumnType.BOOLEAN));
        columns.putAll(creteColumns("column_3", ColumnType.DOUBLE));
        columns.putAll(creteColumns("column_4", ColumnType.FLOAT));
        columns.putAll(creteColumns("column_5", ColumnType.INT));
        columns.putAll(creteColumns("column_6", ColumnType.TEXT));
        columns.putAll(creteColumns("column_7", ColumnType.VARCHAR));

        ClusterName cluteref = new ClusterName(CLUSTER_NAME);

        TableMetadata tableMetadata = new TableMetadata(new TableName(INDEX_NAME, TYPE_NAME), options, columns, indexex,
                cluteref, partitionKey, clusterKey);
        assertEquals("The JSON is correct", RESULT_CREATE_TABLE,
                (deepContentBuilder.createTypeSource(tableMetadata).string()));

    }

    private Map<ColumnName, ColumnMetadata> creteColumns(String columnName, ColumnType columnType) {
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        ColumnName column = new ColumnName(INDEX_NAME, TYPE_NAME, columnName);
        columns.put(column, new ColumnMetadata(column, null, columnType));

        return columns;
    }

}
