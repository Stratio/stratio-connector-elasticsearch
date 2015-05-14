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

package com.stratio.connector.elasticsearch.core.engine.metadata;

import com.stratio.crossdata.common.data.AlterOptions;
import com.stratio.crossdata.common.exceptions.ExecutionException;

/**
 * This class must created a object to alter a table. Created by jmgomez on 24/11/14.
 */
public final class AlterTableFactory {

    /**
     * The constructor.
     */
    private AlterTableFactory() {

    }

    /**
     * Create the correct alter table handler.
     *
     * @param alterOptions
     *            the alter options.
     * @return the correct alter table handler.
     * @throws ExecutionException
     *             if the operation is not supported.
     */
    public static AlterTableHandler createHandler(AlterOptions alterOptions) throws ExecutionException {
        AlterTableHandler handler;
        switch (alterOptions.getOption()) {
        case ADD_COLUMN:
            handler = new AddColumnHandler(alterOptions);
            break;
        default:
            throw new ExecutionException("The altar table operation " + alterOptions.getOption().name() + " "
                            + "is not supporting");
        }
        return handler;
    }
}
