/**
* Copyright (C) 2014 Stratio (http://stratio.com)
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.stratio.connector.elasticsearch.ftest.functionalMetadataEngine;

import org.junit.Assert;
import org.junit.Test;

import com.stratio.connector.elasticsearch.core.engine.ElasticsearchMetadataEngine;
import com.stratio.connector.elasticsearch.ftest.ConnectionTest;
import com.stratio.connector.meta.exception.UnsupportedOperationException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.exceptions.ValidationException;

/**
 * @author darroyo
 *
 */
public class SetAndGetMetadata  extends ConnectionTest{



	    @Test
	    public void setAndGet() throws ExecutionException, ValidationException, UnsupportedOperationException, UnsupportedException {

	    	 
	        ((ElasticsearchMetadataEngine) stratioElasticConnector.getMetadataEngine()).put("1", "metadata");
	        ((ElasticsearchMetadataEngine) stratioElasticConnector.getMetadataEngine()).put("2", "metadata");
	        ((ElasticsearchMetadataEngine) stratioElasticConnector.getMetadataEngine()).put("2", "metadata2");

	        //UPDATE NAME STORAGE 
	        refresh("metadata_storage");
	        
	        String getId1 = ((ElasticsearchMetadataEngine) stratioElasticConnector.getMetadataEngine()).get("1");
	        String getId2 = ((ElasticsearchMetadataEngine) stratioElasticConnector.getMetadataEngine()).get("2");
	        String getId3 = ((ElasticsearchMetadataEngine) stratioElasticConnector.getMetadataEngine()).get("3");
	        
	      
	        
	        Assert.assertEquals("set/get", "metadata",getId1);	        
	        Assert.assertEquals("set and update", "metadata2",  getId2);
	        Assert.assertNull(getId3);
	        
	        deleteSet("metadata_storage");
	    }
	    
	    @Test
	    public void testLargeId() throws ExecutionException, ValidationException, UnsupportedOperationException, UnsupportedException {

	    	 
	        ((ElasticsearchMetadataEngine) stratioElasticConnector.getMetadataEngine()).put("01234567891-.*abcdefghijklmnopqrstvwxyz", "metadata");
	        

	        //UPDATE NAME STORAGE 
	        refresh("metadata_storage");
	        
	        String getId1 = ((ElasticsearchMetadataEngine) stratioElasticConnector.getMetadataEngine()).get("01234567891-.*abcdefghijklmnopqrstvwxyz");

	        Assert.assertEquals("set/get", "metadata",getId1);	        
	    
	        deleteSet("metadata_storage");
	    }
}
