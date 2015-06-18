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

package com.stratio.connector.elasticsearch.core.engine.query.functions;

import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.FunctionRelation;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;
import org.elasticsearch.index.query.QueryBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class FuzzyMultiMatchTest {


    @Test
    public void testMatch() throws UnsupportedException {

        List<Selector> parameters = new ArrayList<>();
        TableName tableName = new TableName("catalog", "table");
        parameters.add(new ColumnSelector(new ColumnName(tableName, "colName")));
        parameters.add(new ColumnSelector(new ColumnName(tableName, "col2Name")));
        parameters.add(new StringSelector("*_colName"));
        parameters.add(new StringSelector("value"));
        parameters.add(new StringSelector("100%"));

        FunctionRelation function = new FunctionRelation(ESFunction.MULTI_MATCH_FUZZY, parameters);
        ESFunction match = ESFunction.build(function);

        //Experimentation
        QueryBuilder builder = match.buildQuery();

        //Expectations
        String expedted = "{\"multi_match\":{\"query\":\"value\",\"fields\":[\"colName\",\"col2Name\",\"*_colName\"],\"fuzziness\":\"100%\"}}";
        Assert.assertEquals(expedted, builder.toString().replace(" ", "").replace("\n", ""));
    }
}
