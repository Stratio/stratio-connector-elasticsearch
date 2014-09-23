/*
 * Stratio Meta
 *
 *   Copyright (c) 2014, Stratio, All rights reserved.
 *
 *   This library is free software; you can redistribute it and/or modify it under the terms of the
 *   GNU Lesser General Public License as published by the Free Software Foundation; either version
 *   3.0 of the License, or (at your option) any later version.
 *
 *   This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 *   even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *   Lesser General Public License for more details.
 *
 *   You should have received a copy of the GNU Lesser General Public License along with this library.
 */

package com.stratio.connector.elasticsearch.ftest.functionalInsert;

import com.stratio.connector.commons.ftest.functionalInsert.GenericSimpleInsertTest;
import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.connector.elasticsearch.ftest.helper.ESConnectorHelper;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;

import static org.junit.Assert.fail;


/**
 * Created by jmgomez on 4/09/14.
 */
public class ESSimpleInsertTest extends GenericSimpleInsertTest {


    @Override
    public void testInsertLong() throws UnsupportedException, ExecutionException {
         fail("Not yet ElasticSearch supported");
    }

    @Override
    public void testInsertDate() throws UnsupportedException, ExecutionException {
        fail("Not yet ElasticSearch supported");
    }

    @Override
    protected IConnectorHelper getConnectorHelper() {
        ESConnectorHelper esConnectorHelper = null;
        try {
            esConnectorHelper = new ESConnectorHelper(getClusterName());
        } catch (InitializationException e) {
            e.printStackTrace();
        } catch (ConnectionException e) {
            e.printStackTrace();
        }
        return esConnectorHelper;
    }
}
