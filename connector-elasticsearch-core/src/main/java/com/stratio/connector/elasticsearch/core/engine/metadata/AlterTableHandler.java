package com.stratio.connector.elasticsearch.core.engine.metadata;

import org.elasticsearch.client.Client;

import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.UnsupportedException;

/**
 * Created by jmgomez on 24/11/14.
 */
public interface AlterTableHandler {
    void execute(TableName tableName, Client connection) throws UnsupportedException;
}
