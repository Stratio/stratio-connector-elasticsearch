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
package com.stratio.connector.elasticsearch.core.engine.utils;

import java.util.List;

import org.elasticsearch.action.search.SearchRequestBuilder;

import com.stratio.connector.meta.Limit;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;

/**
 * @author darroyo
 *
 */
public class ProjectModifier{

	private ProjectModifier(){}
	public static void modify(SearchRequestBuilder requestBuilder, Project projection) {
        List<ColumnMetadata> columnMetadataList  = null;

        //REVIEW comentado por que no existen estos metodos.
	/*	requestBuilder.setIndices(projection.getCatalogName()).setTypes(projection.getTableName());
		
		List<ColumnMetadata> columnMetadataList = projection.getColumnList(); */
		
		if(columnMetadataList == null || columnMetadataList.isEmpty() ) {
				//throw new ValidationException? or select *
    	}else{	
    		String[] fields = new String[columnMetadataList.size()];
    		int i=0;
    		for (ColumnMetadata columnMetadata: columnMetadataList){
    			fields[i] = columnMetadata.getColumnName();//TODO o ALIAS??
    			i++;
    		}
    		//TODO IF NOT STORED=> GET_SOURCE => addField(_all). 
    		requestBuilder.addFields(fields);
    	}
	}

}
