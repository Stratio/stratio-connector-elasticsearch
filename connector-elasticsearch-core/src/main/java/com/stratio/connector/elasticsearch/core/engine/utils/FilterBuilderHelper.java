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
package com.stratio.connector.elasticsearch.core.engine.utils;


import com.stratio.connector.meta.exception.UnsupportedOperationException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;
import com.stratio.meta2.common.statements.structures.selectors.IntegerSelector;
import com.stratio.meta2.common.statements.structures.selectors.Selector;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;

import java.util.List;


public class FilterBuilderHelper {

    private FilterBuilderHelper() {
    }

    public static FilterBuilder createFilterBuilder(List<Filter> filters) throws UnsupportedOperationException {


        Relation relation;
        Operator operator;

        FilterBuilder filterBuilder = null;
        BoolFilterBuilder boolFilterBuilder = null;

        boolFilterBuilder = (filters.size() > 1) ? FilterBuilders.boolFilter() : null;

        for (Filter filter : filters) {
            relation = filter.getRelation();
            operator = relation.getOperator();

            FilterBuilder localFilterBuilder = handleCompareFilter(relation);
            if (boolFilterBuilder == null && localFilterBuilder != null) {
                filterBuilder = localFilterBuilder;
            } else {
                // TODO update when or is implemented
                if (operator == Operator.DISTINCT) {
                    boolFilterBuilder.mustNot(localFilterBuilder);
                } else
                    boolFilterBuilder.must(localFilterBuilder);

            }

//			switch (operator) {
//
//			case BETWEEN:
//
//				// TODO update when or is implemented
//				if (boolFilterBuilder == null) filterBuilder = localFilterBuilder;
//				else {
//
//					boolFilterBuilder.must(localFilterBuilder);
//				}
//			break;
//
//			case COMPARE:
//
//				if (boolFilterBuilder == null && localFilterBuilder != null)
//					filterBuilder = localFilterBuilder;
//				else {
//					// TODO update when or is implemented
//					if (operator == Operator.DISTINCT) {
//						boolFilterBuilder.mustNot(localFilterBuilder);
//					} else
//						boolFilterBuilder.must(localFilterBuilder);
//
//				}
//
//				break;
//
//			case IN:
//
//
//
//
//				if (boolFilterBuilder == null)
//					filterBuilder = localFilterBuilder;
//				else {
//					// TODO update when or is implemented
//					boolFilterBuilder.must(localFilterBuilder);
//				}
//
//				break;
//
//			// case TOKEN:
//			// TODO get instead of search
//			// //GetResponse response = client.prepareGet("twitter", "tweet",
//			// "1")
//			// // .execute()
//			// // .actionGet();
//			// break;
//
//			default: throw new UnsupportedOperationException("Relation type unsupported");
//
//			}
        }


        if (boolFilterBuilder != null) return boolFilterBuilder;
        else return filterBuilder;

    }


    /**
     * @param relation
     * @return
     */
    private static FilterBuilder handleInFilter(Relation relation) {
//		Relation relIn = (RelationIn) relation;
//		// check integer, number, string,date, etc...??
//
//		ArrayList inTerms = new ArrayList();
//		for (Term<?> term : relIn.getTerms()) {
//			// TODO check if insert the field or not
//			inTerms.add(term.getTermValue());
//		}
//
//		return FilterBuilders.inFilter(relation.getIdentifiers().get(0).getField(),inTerms.toArray());
        throw new RuntimeException("A la espera de que se implemente por Meta"); //REVIEW

    }

    private static FilterBuilder handleCompareFilter(Relation relation) throws UnsupportedOperationException {

        FilterBuilder localFilterBuilder = null;
        // TermFilter: Filters documents that have fields that contain a
        // term (not analyzed)

        Selector leftTerm = relation.getLeftTerm();
        Selector rightTerm = relation.getRightTerm();
        switch (relation.getOperator()) {
            case COMPARE:
            case DISTINCT /*The not is modify in FilterBuilder method */: //REVIEW el distinct
                localFilterBuilder = FilterBuilders.termFilter(getSelectorField(leftTerm), getSelectorField(rightTerm));
                break;
            case LT:
                localFilterBuilder = FilterBuilders.rangeFilter(getSelectorField(leftTerm)).lt(getSelectorField(rightTerm));
                break;
            case LET:
                localFilterBuilder = FilterBuilders.rangeFilter(getSelectorField(leftTerm)).lte(getSelectorField(rightTerm));
                break;
            case GT:
                localFilterBuilder = FilterBuilders.rangeFilter(getSelectorField(leftTerm)).gt(getSelectorField(rightTerm));
                break;
            case GET:
                localFilterBuilder = FilterBuilders.rangeFilter(getSelectorField(leftTerm)).gte(getSelectorField(rightTerm));
                break;
            case BETWEEN:
                new RuntimeException("A la espera de que se implemente por Meta"); //REVIEW mewtodo  handleBetweenFilter
            default:
                throw new UnsupportedOperationException("Not implemented yet.");  //TODO

        }


        return localFilterBuilder;

    }

    private static String getSelectorField(Selector selector) {
        String field = "";
        if (selector instanceof ColumnSelector) {
            ColumnSelector columnSelector = (ColumnSelector) selector;
            field = columnSelector.getName().getName();
        } else if (selector instanceof IntegerSelector) {
            IntegerSelector integerSelector = (IntegerSelector) selector;
            field = String.valueOf(integerSelector.getValue());
        } else throw new RuntimeException("Not implemented yet.");//TODO


        return field;
    }


    private static FilterBuilder handleBetweenFilter(Relation relation) {

//		RelationBetween relBetween = (RelationBetween) relation;
//		return FilterBuilders.rangeFilter(relation.getIdentifiers().get(0).getField())
//				.gte(relBetween.getTerms().get(0).getTermValue())
//				.lte(relBetween.getTerms().get(1).getTermValue());

        throw new RuntimeException("A la espera de que se implemente por Meta"); //REVIEW
    }


}
