package com.stratio.connector.elasticsearch.core.engine.query.metdata; 

import static junit.framework.TestCase.assertEquals;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test; 
import org.junit.Before; 
import org.junit.After;

import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryData;
import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.ColumnType;

/** 
* MetadataCreator Tester. 
* 
* @author <Authors name> 
* @since <pre>oct 15, 2014</pre> 
* @version 1.0 
*/

public class MetadataCreatorTest {

    private static final String CATALOG_NAME = "catalog_name";
    private static final String TABLE_NAME = "table_name";
    private static final String COLUMN_NAME1 = "column_name1";
    private static final String ALIAS_1 = "alias_1";

    private static final String COLUMN_NAME2 = "column_name2";
    private static final String ALIAS_2 = "alias_2";

    private static final String COLUMN_NAME3 = "column_name3";
    private static final String ALIAS_3 = "alias_3";

    MetadataCreator metadataCreator;

    private String[] ALIAS = {ALIAS_1,ALIAS_2,ALIAS_3 };
    private String[] NAMES = {COLUMN_NAME1,COLUMN_NAME2,COLUMN_NAME3};

    private ColumnType[] TYPES = {ColumnType.BOOLEAN,ColumnType.DOUBLE,ColumnType.VARCHAR};

    @Before
public void before() throws Exception {

        metadataCreator = new MetadataCreator();
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: createMetadata(ConnectorQueryData queryData) 
* 
*/


@Test
public void testCreateMetadata() throws Exception {
    ConnectorQueryData queryData = createQueryData();
    List<ColumnMetadata> columnMetadata = metadataCreator.createMetadata(queryData);

    int i = 0;
    for (ColumnMetadata metadata : columnMetadata){

        assertEquals("Alias is correct", ALIAS[i], metadata.getColumnAlias());
        assertEquals("Column name is correct", CATALOG_NAME+"."+TABLE_NAME+"."+NAMES[i],metadata.getColumnName());
        assertEquals("Table name is correct", CATALOG_NAME+"."+TABLE_NAME,metadata.getTableName());
        assertEquals("Type name is correct", TYPES[i], metadata.getType());
        i++;

    }
}


    private ConnectorQueryData createQueryData() {
        ConnectorQueryData queryData = new ConnectorQueryData();
        Map<ColumnName, String> columnMap = new LinkedHashMap();
        columnMap.put(new ColumnName(CATALOG_NAME,TABLE_NAME,NAMES[0]), ALIAS[0]);
        columnMap.put(new ColumnName(CATALOG_NAME,TABLE_NAME,NAMES[1]), ALIAS[1]);
        columnMap.put(new ColumnName(CATALOG_NAME,TABLE_NAME,NAMES[2]),ALIAS[2]);
        Map<String,ColumnType> typeMap = new LinkedHashMap<>();
        typeMap.put(CATALOG_NAME+"."+TABLE_NAME+"."+COLUMN_NAME1, TYPES[0]);
        typeMap.put(CATALOG_NAME+"."+TABLE_NAME+"."+COLUMN_NAME2, TYPES[1]);
        typeMap.put(CATALOG_NAME+"."+TABLE_NAME+"."+COLUMN_NAME3, TYPES[2]);
        Select select = new Select(Operations.SELECT_OPERATOR,columnMap,typeMap);
        Project project = new Project(Operations.PROJECT,new TableName(CATALOG_NAME,TABLE_NAME),
                new ClusterName("CLUSTER_NAME"));
        queryData.setProjection(project);
        queryData.setSelect(select);
        return queryData;
    }

} 
