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

import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;

/**
 * Created by lcisneros on 2/06/15.
 */
public class Match extends ESFunction{



    protected Match(List<Selector> paramareters) {
        super("contains", paramareters);

    }

    @Override
    public QueryBuilder buildQuery() {
        String field = getParameters().get(0).getStringValue();
        String value = getParameters().get(1).getStringValue();
        return QueryBuilders.matchQuery(field, value.toLowerCase());
    }
}
