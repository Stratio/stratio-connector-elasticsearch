package com.stratio.connector.elasticsearch.ftest.functionalMetadata;

import com.stratio.connector.commons.ftest.functionalMetadata.GenericMetadataAlterTableFT;
import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.connector.elasticsearch.ftest.helper.ESConnectorHelper;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.InitializationException;

/**
 * Created by jmgomez on 24/11/14.
 */
public class ESMetadataAlterTableFT extends GenericMetadataAlterTableFT {

    @Override
    protected IConnectorHelper getConnectorHelper() {
        ESConnectorHelper esConnectorHelper = null;
        try {
            esConnectorHelper = new ESConnectorHelper(getClusterName());
            return esConnectorHelper;
        } catch (ConnectionException e) {
            e.printStackTrace();
        } catch (InitializationException e) {
            e.printStackTrace();
        }
        return esConnectorHelper;
    }
}
