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

import java.util.ArrayList;
import java.util.List;

import com.stratio.meta2.common.statements.structures.terms.Term;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;

import com.stratio.connector.meta.exception.UnsupportedOperationException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta.common.statements.structures.relationships.RelationBetween;
import com.stratio.meta.common.statements.structures.relationships.RelationCompare;
import com.stratio.meta.common.statements.structures.relationships.RelationIn;
import com.stratio.meta.common.statements.structures.relationships.RelationType;


public class FilterBuilderHelper {

	private FilterBuilderHelper() {
	}

	public static FilterBuilder createFilterBuilder(List<Filter> filters) throws UnsupportedOperationException {

		
		RelationType relationType;
		Relation relation;

		FilterBuilder filterBuilder = null;
		BoolFilterBuilder boolFilterBuilder = null;

		boolFilterBuilder = (filters.size() > 1) ? FilterBuilders.boolFilter() : null ;

		for (Filter filter : filters) {
			relationType = filter.getType();
			relation = filter.getRelation();
			
			FilterBuilder localFilterBuilder = null;
			
			switch (relationType) {

			case BETWEEN:
				localFilterBuilder = handleBetweenFilter(relation);
				// TODO update when or is implemented
				if (boolFilterBuilder == null) filterBuilder = localFilterBuilder;
				else {
					
					boolFilterBuilder.must(localFilterBuilder);
				}
			break;

			case COMPARE:

				
				String operator = ((RelationCompare) relation).getOperator();
				localFilterBuilder = handleCompareFilter(relation);
				
				if (boolFilterBuilder == null && localFilterBuilder != null)
					filterBuilder = localFilterBuilder;
				else {
					// TODO update when or is implemented
					if (operator.equals("<>") || operator.equals("!=")) {
						boolFilterBuilder.mustNot(localFilterBuilder);
					} else
						boolFilterBuilder.must(localFilterBuilder);

				}

				break;

			case IN:

				localFilterBuilder = handleInFilter(relation);
				

				if (boolFilterBuilder == null)
					filterBuilder = localFilterBuilder;
				else {
					// TODO update when or is implemented
					boolFilterBuilder.must(localFilterBuilder);
				}

				break;

			// case TOKEN:
			// TODO get instead of search
			// //GetResponse response = client.prepareGet("twitter", "tweet",
			// "1")
			// // .execute()
			// // .actionGet();
			// break;

			default: throw new UnsupportedOperationException("Relation type unsupported");

			}			
			
		}
		
		if (boolFilterBuilder != null) return boolFilterBuilder;
		else return filterBuilder;

	}
	
	
	/**
	 * @param relation
	 * @return
	 */
	private static FilterBuilder handleInFilter(Relation relation) {
		RelationIn relIn = (RelationIn) relation;
		// check integer, number, string,date, etc...??

		ArrayList inTerms = new ArrayList();
		for (Term<?> term : relIn.getTerms()) {
			// TODO check if insert the field or not
			inTerms.add(term.getTermValue());
		}

		return FilterBuilders.inFilter(relation.getIdentifiers().get(0).getField(),inTerms.toArray());

	}

	private static FilterBuilder handleCompareFilter(Relation relation) {
		
		FilterBuilder localFilterBuilder = null;
		// TermFilter: Filters documents that have fields that contain a
		// term (not analyzed)

		RelationCompare relCompare = (RelationCompare) relation;

		if (relCompare.getOperator().equals("="))
			localFilterBuilder = FilterBuilders.termFilter(
					relation.getIdentifiers().get(0).getField(), 
					relCompare.getTerms().get(0).getTermValue());
		
		else if (relCompare.getOperator().equals(">"))
			localFilterBuilder = FilterBuilders.rangeFilter(
					relation.getIdentifiers().get(0).getField()).gt(
					relCompare.getTerms().get(0).getTermValue());
		
		else if (relCompare.getOperator().equals(">="))
			localFilterBuilder = FilterBuilders.rangeFilter(
					relation.getIdentifiers().get(0).getField()).gte(
					relCompare.getTerms().get(0).getTermValue());
		
		else if (relCompare.getOperator().equals("<"))
			localFilterBuilder = FilterBuilders.rangeFilter(
					relation.getIdentifiers().get(0).getField()).lt(
					relCompare.getTerms().get(0).getTermValue());
		
		else if (relCompare.getOperator().equals("<="))
			localFilterBuilder = FilterBuilders.rangeFilter(
					relation.getIdentifiers().get(0).getField()).lte(
					relCompare.getTerms().get(0).getTermValue());
		
		else if (relCompare.getOperator().equals("<>")
				|| relCompare.getOperator().equals("!="))
			localFilterBuilder = FilterBuilders.termFilter(relation
					.getIdentifiers().get(0).getField(), relCompare
					.getTerms().get(0).getTermValue());
		
		return localFilterBuilder;

	}


	private static FilterBuilder handleBetweenFilter(Relation relation) {
		
		RelationBetween relBetween = (RelationBetween) relation;
		return FilterBuilders.rangeFilter(relation.getIdentifiers().get(0).getField())
				.gte(relBetween.getTerms().get(0).getTermValue())
				.lte(relBetween.getTerms().get(1).getTermValue());
	}

	
}
