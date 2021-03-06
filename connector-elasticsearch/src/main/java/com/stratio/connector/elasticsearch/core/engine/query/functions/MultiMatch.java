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

import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.ArrayList;
import java.util.List;

public class MultiMatch extends ESFunction {

    protected MultiMatch(List<Selector> parameters) {
        super(ESFunction.MULTI_MATCH, parameters);
    }

    @Override
    public QueryBuilder buildQuery() {

        String minimumShouldMatch = getParameters().get(getParameters().size() -1).getStringValue();
        String value = getParameters().get(getParameters().size() -2).getStringValue();
        String[] fields = new String[getParameters().size() -2];

        for (int i = 0; i < getParameters().size() -2; i++){
            if (getParameters().get(i) instanceof ColumnSelector){
                fields[i]=((ColumnSelector) getParameters().get(i)).getColumnName().getName();
            }else{
                fields[i]= getParameters().get(i).getStringValue();
            }
        }

        return QueryBuilders.multiMatchQuery(value, fields).minimumShouldMatch(minimumShouldMatch);
    }
}