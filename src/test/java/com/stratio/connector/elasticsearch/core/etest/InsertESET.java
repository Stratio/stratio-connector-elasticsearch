package com.stratio.connector.elasticsearch.core.etest;

import com.stratio.connector.commons.etest.InsertETest;
import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.connector.elasticsearch.core.ftest.helper.ESConnectorHelper;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.InitializationException;

public class InsertESET extends InsertETest{

    private ESConnectorHelper esConnectorHelper = null;
    
	   @Override
	    protected IConnectorHelper getConnectorHelper() {
	        try {
	            if (esConnectorHelper == null) {
	                esConnectorHelper = new ESConnectorHelper(getClusterName());
	            }

	            return esConnectorHelper;
	        } catch (ConnectionException e) {
	            e.printStackTrace();
	        } catch (InitializationException e) {
	            e.printStackTrace();
	        }
	        return esConnectorHelper;
	    }

}
