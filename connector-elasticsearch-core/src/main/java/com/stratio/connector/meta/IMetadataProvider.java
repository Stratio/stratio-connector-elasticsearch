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

import com.stratio.connector.meta.exception.UnsupportedOperationException;
import com.stratio.meta.common.exceptions.ExecutionException;

import java.io.Serializable;

/**
 * Created by jmgomez on 10/07/14.
 */
public interface IMetadataProvider extends Serializable {

    public void createCatalog(String catalog) throws UnsupportedOperationException;

    public void createTable(String catalog, String table) throws UnsupportedOperationException;

    public void dropCatalog(String catalog) throws UnsupportedOperationException, ExecutionException;

    public void dropTable(String catalog, String table) throws UnsupportedOperationException, ExecutionException;

    public void createIndex(String catalog, String table, String... field) throws UnsupportedOperationException;

    public void dropIndex(String catalog, String table, String... field) throws UnsupportedOperationException;

    public void dropIndexes(String catalog, String table) throws UnsupportedOperationException;
}
