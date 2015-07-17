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

import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;

public class Fuzzy extends ESFunction{

    private final String FUNCTION_NAME = "fuzzy";
    private final String EXPECTED_SIGNATURE = "fuzzy (fields, value, fuzziness)";
    private final int EXPECTED_PARAMETERS = 3;

    protected Fuzzy(List<Selector> parameters) {
        super(ESFunction.FUZZY, parameters);
    }

    @Override
    public QueryBuilder buildQuery() throws ExecutionException {

        // Checks function signature
        validateFunctionSignature(FUNCTION_NAME, true, EXPECTED_PARAMETERS, EXPECTED_SIGNATURE);

        // Retrieves function parameters

        // Retrieves the string to be seached
        String value = getParameters().get(1).getStringValue();

        // Retrieves the fuzziness value
        String fuzziness = getParameters().get(2).getStringValue();

        // Retrieves the fields to be searched in
        String fields = "";

        // If first parameter is a column its name is set as the search field
        if (getParameters().get(0) instanceof ColumnSelector ){
            fields = ((ColumnSelector) getParameters().get(0)).getColumnName().getName();
        }
        // Otherwise the received value is treated as the search fields string
        else {
            fields = getParameters().get(0).getStringValue();
        }

        // Once the search fields string is retrieved it is checked if more than one field is queried so we could build a match or multi match query
        if (fields.split("\\s+").length > 1){
            return QueryBuilders.multiMatchQuery(value, fields.split("\\s+")).fuzziness(fuzziness);
        }
        else {
            return QueryBuilders.fuzzyQuery(fields, value).fuzziness(Fuzziness.build(fuzziness));
        }
    }
}
