package com.stratio.connector.elasticsearch.core.engine.metadata;

import java.io.IOException;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.stratio.connector.elasticsearch.core.engine.utils.ContentBuilderCreator;
import com.stratio.crossdata.common.data.AlterOptions;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.UnsupportedException;

/**
 * Created by jmgomez on 24/11/14.
 */
public class AddColumnHandler implements AlterTableHandler {

    private final AlterOptions alterOptions;

    public AddColumnHandler(AlterOptions alterOptions) {
        this.alterOptions = alterOptions;
    }

    @Override public void execute(TableName tableName,Client connection) throws UnsupportedException {



        try {
            ContentBuilderCreator contentBuilderCreator = new ContentBuilderCreator();
            XContentBuilder source = null;
            source = contentBuilderCreator.addColumn(alterOptions.getColumnMetadata());
            connection.admin().indices().preparePutMapping(tableName.getCatalogName().getName()).setType(tableName
                    .getName()).setSource(source).execute().actionGet();

        } catch (IOException e) {
           //TODO
        }

    }
}
