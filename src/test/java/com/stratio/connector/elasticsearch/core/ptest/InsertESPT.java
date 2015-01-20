package com.stratio.connector.elasticsearch.core.ptest;

import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.connector.commons.ptest.storage.insert.InsertOneGenericPT;
import com.stratio.connector.elasticsearch.core.ftest.helper.ESConnectorHelper;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.InitializationException;

public class InsertESPT extends InsertOneGenericPT {

    private ESConnectorHelper esConnectorHelper = null;
    
	   @Override
	    protected IConnectorHelper getConnectorHelper() {
	        try {
	            if (esConnectorHelper == null) {
	                esConnectorHelper = new ESConnectorHelper(getClusterName());
	            }

	            return esConnectorHelper;
	        } catch (ConnectionException | InitializationException  e) {
	            e.printStackTrace();
	        }
	        return esConnectorHelper;
	    }

}
