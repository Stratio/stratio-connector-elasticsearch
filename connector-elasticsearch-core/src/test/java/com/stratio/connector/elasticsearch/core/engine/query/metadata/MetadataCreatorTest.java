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

package com.stratio.connector.elasticsearch.core.engine.query.metadata;

import static junit.framework.TestCase.assertEquals;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryData;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.structures.ColumnMetadata;

/**
 * MetadataCreator Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>oct 15, 2014</pre>
 */

public class MetadataCreatorTest {

    private static final String CATALOG_NAME = "catalog_name";
    private static final String TABLE_NAME = "table_name";
    private static final String COLUMN_NAME1 = "column_name1";
    private static final String ALIAS_1 = "alias_1";

    private static final String COLUMN_NAME2 = "column_name2";
    private static final String ALIAS_2 = "alias_2";

    private static final String COLUMN_NAME3 = "column_name3";
    private String[] NAMES = { COLUMN_NAME1, COLUMN_NAME2, COLUMN_NAME3 };
    private static final String ALIAS_3 = "alias_3";
    private String[] ALIAS = { ALIAS_1, ALIAS_2, ALIAS_3 };
    MetadataCreator metadataCreator;
    private ColumnType[] TYPES = { ColumnType.BOOLEAN, ColumnType.DOUBLE, ColumnType.VARCHAR };

    @Before
    public void before() throws Exception {

        metadataCreator = new MetadataCreator();
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: createColumnMetadata(ConnectorQueryData queryData)
     */

    @Test
    public void testCreateMetadata() throws Exception {
        ConnectorQueryData queryData = createQueryData();
        List<ColumnMetadata> columnMetadata = metadataCreator.createColumnMetadata(queryData);

        int i = 0;
        for (ColumnMetadata metadata : columnMetadata) {

            assertEquals("Alias is correct", ALIAS[i], metadata.getColumnAlias());
            assertEquals("Column name is correct", CATALOG_NAME + "." + TABLE_NAME + "." + NAMES[i],
                    metadata.getColumnName());
            assertEquals("Table name is correct", CATALOG_NAME + "." + TABLE_NAME, metadata.getTableName());
            assertEquals("Type name is correct", TYPES[i], metadata.getType());
            i++;

        }
    }

    private ConnectorQueryData createQueryData() {
        ConnectorQueryData queryData = new ConnectorQueryData();
        Map<ColumnName, String> columnMap = new LinkedHashMap();
        columnMap.put(new ColumnName(CATALOG_NAME, TABLE_NAME, NAMES[0]), ALIAS[0]);
        columnMap.put(new ColumnName(CATALOG_NAME, TABLE_NAME, NAMES[1]), ALIAS[1]);
        columnMap.put(new ColumnName(CATALOG_NAME, TABLE_NAME, NAMES[2]), ALIAS[2]);
        Map<String, ColumnType> typeMap = new LinkedHashMap<>();
        typeMap.put(ALIAS[0], TYPES[0]);
        typeMap.put(ALIAS[1], TYPES[1]);
        typeMap.put(ALIAS[2], TYPES[2]);

        Map<ColumnName, ColumnType> typeColumnName = new LinkedHashMap<>();
        typeColumnName.put(new ColumnName(CATALOG_NAME ,TABLE_NAME , COLUMN_NAME1), TYPES[0]);
        typeColumnName.put(new ColumnName(CATALOG_NAME, TABLE_NAME , COLUMN_NAME2), TYPES[1]);
        typeColumnName.put(new ColumnName(CATALOG_NAME ,TABLE_NAME, COLUMN_NAME3), TYPES[2]);
        Select select = new Select(Operations.SELECT_OPERATOR, columnMap, typeMap,typeColumnName);
        Project project = new Project(Operations.PROJECT, new TableName(CATALOG_NAME, TABLE_NAME),
                new ClusterName("CLUSTER_NAME"));
        queryData.setProjection(project);
        queryData.setSelect(select);
        return queryData;
    }

} 
