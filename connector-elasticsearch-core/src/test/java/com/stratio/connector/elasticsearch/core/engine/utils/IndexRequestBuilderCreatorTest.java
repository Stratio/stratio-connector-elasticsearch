package com.stratio.connector.elasticsearch.core.engine.utils;


import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.elasticsearch.core.connection.ElasticSearchConnectionHandler;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.IndexName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.ColumnMetadata;
import com.stratio.meta2.common.metadata.IndexMetadata;
import com.stratio.meta2.common.metadata.TableMetadata;
import com.stratio.meta2.common.statements.structures.selectors.Selector;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * IndexRequestBuilderCreator Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>sep 12, 2014</pre>
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(value = {Client.class})
public class IndexRequestBuilderCreatorTest {


    private static final String CLUSTER_NAME = "CLUSTER NAME";
    private static final String INDEX_NAME = "INDEX_NAME";
    private static final String TYPE_NAME = "TYPE_NAME";
    private TableName tableMame = new TableName(INDEX_NAME, TYPE_NAME);
    private static final String COLUMN_NAME = "row_name";
    private static final String OTHER_ROW_NAME = "OTHER_ROW_NAME";
    private static final String CELL_VALUE = "cell_value";

    private static final Integer INTEGER_CELL_VALUE = new Integer(5);
    @Rule
    public ExpectedException exception = ExpectedException.none();
    IndexRequestBuilderCreator indexRequestBuilderCreator;
    private Map<ColumnName, ColumnMetadata> columns = null;
    private Map<Selector, Selector> options = null;
    private Map<IndexName, IndexMetadata> indexes = null;
    private ClusterName clusterRef = null;
    private List<ColumnName> partirionKey = Collections.emptyList();
    private List<ColumnName> clusterKey = Collections.emptyList();
    @Mock
    private ElasticSearchConnectionHandler connectionHandler;
    @Mock
    private Connection<Client> connection;
    @Mock
    private Client client;

    @Before
    public void before() throws Exception {
        indexRequestBuilderCreator = new IndexRequestBuilderCreator();
    }

    @After
    public void after() throws Exception {
    }


    @Test
    public void createIndexRequestBuilderTest() throws UnsupportedException {


        partirionKey = new ArrayList<>();
        partirionKey.add(new ColumnName(INDEX_NAME, TYPE_NAME, COLUMN_NAME));
        TableMetadata targetTable = new TableMetadata(tableMame, options, columns, indexes, clusterRef, partirionKey, clusterKey);
        Row row = createRow(COLUMN_NAME, CELL_VALUE);
        row.addCell(COLUMN_NAME, new Cell(CELL_VALUE));

        partirionKey = new ArrayList<>();
        partirionKey.add(new ColumnName(INDEX_NAME, TYPE_NAME, COLUMN_NAME));


        Map<String, Object> dataInsert = new HashMap<>();
        dataInsert.put(COLUMN_NAME, CELL_VALUE);

        Map other = new HashMap<>();
        other.put(COLUMN_NAME, CELL_VALUE);


        IndexRequestBuilder indexRequiestBuilder = mock(IndexRequestBuilder.class);
        when(indexRequiestBuilder.setSource(eq(other))).thenReturn(indexRequiestBuilder);
        when(client.prepareIndex(INDEX_NAME, TYPE_NAME, CELL_VALUE)).thenReturn(indexRequiestBuilder);

        IndexRequestBuilder indexRequestBuilder = indexRequestBuilderCreator.createIndexRequestBuilder(targetTable, client, row);

        assertNotNull("The index request builder is not null", indexRequestBuilder);


    }


    @Test
    public void createIndexRequestBuilderWithPkTest() throws UnsupportedException {


        TableMetadata targetTable = new TableMetadata(tableMame, options, columns, indexes, clusterRef, partirionKey, clusterKey);
        Row row = createRow(COLUMN_NAME, CELL_VALUE);
        row.addCell(COLUMN_NAME, new Cell(CELL_VALUE));

        partirionKey = new ArrayList<>();
        partirionKey.add(new ColumnName(INDEX_NAME, TYPE_NAME, COLUMN_NAME));


        Map<String, Object> dataInsert = new HashMap<>();
        dataInsert.put(COLUMN_NAME, CELL_VALUE);

        Map other = new HashMap<>();
        other.put(COLUMN_NAME, CELL_VALUE);


        IndexRequestBuilder indexRequiestBuilder = mock(IndexRequestBuilder.class);
        when(indexRequiestBuilder.setSource(eq(other))).thenReturn(indexRequiestBuilder);
        when(client.prepareIndex(INDEX_NAME, TYPE_NAME)).thenReturn(indexRequiestBuilder);

        IndexRequestBuilder indexRequestBuilder = indexRequestBuilderCreator.createIndexRequestBuilder(targetTable, client, row);

        assertNotNull("The index request builder is not null", indexRequestBuilder);


    }


    private Row createRow(String rowKey, Object cellValue) {
        Cell cell = new Cell(cellValue);
        Row row = new Row(rowKey, cell);
        return row;
    }


}
