package com.stratio.connector.elasticsearch.core.engine.metadata;

import com.stratio.crossdata.common.data.AlterOptions;
import com.stratio.crossdata.common.exceptions.UnsupportedException;

/**
 * Created by jmgomez on 24/11/14.
 */
public class AlterTableFactory {
    public static AlterTableHandler createHandeler(AlterOptions alterOptions) throws UnsupportedException {
        AlterTableHandler handler;
        switch (alterOptions.getOption()){
            case ADD_COLUMN: handler = new AddColumnHandler(alterOptions);break;
            default: throw new UnsupportedException("The altar table operation "+alterOptions.getOption().name() +" " +
                    "is not supporting");
        }
        return handler;
    }
}
