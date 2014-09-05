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


package com.stratio.connector.meta;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class EResultSetIterator implements Iterator<com.stratio.meta.common.data.Row> {

    /**
     * Set representing a result from Mongo.
     */
    private final ElasticsearchResultSet eResultSet;

    /**
     * Pointer to the current element.
     */
    private int current;

    /**
     * Build a {@link com.stratio.meta.common.data.CResultSetIterator} from a {@link com.stratio.meta.common.data.CassandraResultSet}.
     *
     * @param cResultSet Cassandra Result Set.
     */
    public EResultSetIterator(ElasticsearchResultSet eResultSet) {
        this.eResultSet = eResultSet;
        this.current = 0;
    }

    @Override
    public boolean hasNext() {
        return current < eResultSet.getRows().size();
    }

    @Override
    public com.stratio.meta.common.data.Row next() throws NoSuchElementException {
        return eResultSet.getRows().get(current++);
    }

    @Override
    public void remove() throws UnsupportedOperationException, IllegalStateException {
        eResultSet.remove(current);
    }
}
