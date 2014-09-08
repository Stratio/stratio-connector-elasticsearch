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
package com.stratio.connector.elasticsearch.ftest.functionalTestQuery;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.stratio.connector.elasticsearch.core.engine.ElasticsearchQueryEngine;
import com.stratio.connector.elasticsearch.core.engine.ElasticsearchStorageEngine;
import com.stratio.connector.elasticsearch.ftest.ConnectionTest;
import com.stratio.connector.meta.Limit;
import com.stratio.connector.meta.Match;
import com.stratio.connector.meta.Sort;
import com.stratio.connector.meta.exception.UnsupportedOperationException;
import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.LogicalPlan;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta.common.result.QueryResult;


public class QueryMatchTest extends ConnectionTest{

    public static final String COLUMN_NAME = "name";
    public static final String COLUMN_NUMVIEWS = "views";
    public static final String COLUMN_TEXT = "text";

    
    private static final String M_ELEM = "Mercury(element)";
    private static final String M_PLANET = "Mercury(planet)";
    private static final String M_MYTHO = "Mercury(mythology)";
    private static final String M_FRED = "Mercury(freddie)";

    private String freddie ="Freddie Mercury (born Farrokh Bulsara; Gujarati: Pharōkh Balsārā‌; 5 September 1946 – 24 November 1991) was a British musician, record producer, and singer-songwriter, best known as the lead vocalist and lyricist of the rock band Queen. As a performer, he was known for his flamboyant stage persona and powerful vocals over a four-octave range.[3][4][5] As a songwriter, he composed many hits for Queen, including Bohemian Rhapsody, Killer Queen, Somebody to Love, Don't Stop Me Now, Crazy Little Thing Called Love, and We Are the Champions. In addition to his work with Queen, he led a solo career, and also occasionally served as a producer and guest musician (piano or vocals) for other artists. He died of bronchopneumonia brought on by AIDS on 24 November 1991, only one day after publicly acknowledging he had the disease Mercury was a Parsi born in Sultanate of Zanzibar and grew up there and in India until his mid-teens. Posthumously, in 1992 he was awarded the Brit Award for Outstanding Contribution to British Music, and the Freddie Mercury Tribute Concert was held at Wembley Stadium, London. As a member of Queen, he was inducted into the Rock and Roll Hall of Fame in 2001, the Songwriters Hall of Fame in 2003, the UK Music Hall of Fame in 2004, and the band received a star on the Hollywood Walk of Fame in 2002. Also in 2002, Mercury was placed at number 58 in the BBC's poll of the 100 Greatest Britons. He continues to be voted one of the greatest singers in the history of popular music. In 2005, a poll organised by Blender and MTV2 saw Mercury voted the greatest male singer of all time.[6] In 2008, Rolling Stone editors ranked him number 18 on their list of the 100 greatest singers of all time.[5] In 2009, a Classic Rock poll saw him voted the greatest rock singer of all time.[7] AllMusic has characterised Mercury as one of rock's greatest all-time entertainers, who possessed one of the greatest voices in all of music";
    private String element = "Mercury is a chemical element with the symbol Hg and atomic number 80. It is commonly known as quicksilver and was formerly named hydrargyrum (/haɪˈdrɑrdʒərəm/).[2] A heavy, silvery d-block element, mercury is the only metallic element that is liquid at standard conditions for temperature and pressure; the only other element that is liquid under these conditions is bromine, though metals such as caesium, gallium, and rubidium melt just above room temperature.Mercury occurs in deposits throughout the world mostly as cinnabar (mercuric sulfide). The red pigment vermilion, a pure form of mercuric sulfide, is mostly obtained by reaction of mercury (produced by reduction from cinnabar) with sulfur. Mercury poisoning can result from exposure to water-soluble forms of mercury (such as mercuric chloride or methylmercury), inhalation of mercury vapor, or eating seafood contaminated with mercury. Mercury is used in thermometers, barometers, manometers, sphygmomanometers, float valves, mercury switches, mercury relays, fluorescent lamps and other devices, though concerns about the element's toxicity have led to mercury thermometers and sphygmomanometers being largely phased out in clinical environments in favour of alternatives such as alcohol- or galinstan-filled glass thermometers and thermistor- or infrared-based electronic instruments. Likewise, mechanical pressure gauges and electronic strain gauge sensors have replaced mercury sphygmomanometers. Mercury remains in use in scientific research applications and in amalgam material for dental restoration in some locales. It is used in lighting: electricity passed through mercury vapor in a fluorescent lamp produces short-wave ultraviolet light which then causes the phosphor in the tube to fluoresce, making visible light.";
    private String planet = "Mercury is the smallest and closest to the Sun of the eight planets in the Solar System,[a] with an orbital period of about 88 Earth days. Seen from Earth, it appears to move around its orbit in about 116 days, which is much faster than any other planet. This rapid motion may have led to it being named after the Roman deity Mercury, the fast-flying messenger to the gods. Because it has almost no atmosphere to retain heat, Mercury's surface experiences the greatest temperature variation of all the planets, ranging from 100 K (−173 °C; −280 °F) at night to 700 K (427 °C; 800 °F) during the day at some equatorial regions. The poles are constantly below 180 K (−93 °C; −136 °F). Mercury's axis has the smallest tilt of any of the Solar System's planets (about 1⁄30 of a degree), but it has the largest orbital eccentricity.[a] As such it does not experience seasons in the same way as most other planets such as Earth. At aphelion, Mercury is about 1.5 times as far from the Sun as it is at perihelion. Mercury's surface is heavily cratered and similar in appearance to the Moon, indicating that it has been geologically inactive for billions of years.Mercury is gravitationally locked and rotates in a way that is unique in the Solar System. As seen relative to the fixed stars, it rotates exactly three times for every two revolutions[b] it makes around its orbit.[13] As seen from the Sun, in a frame of reference that rotates with the orbital motion, it appears to rotate only once every two Mercurian years. An observer on Mercury would therefore see only one day every two years.Because Mercury moves in an orbit around the Sun which lies within Earth's orbit (as does Venus), it can appear in Earth's sky in the morning or the evening, but not in the middle of the night. Also, like Venus and the Moon, it displays a complete range of phases as it moves around its orbit relative to Earth. Although Mercury can appear as a very bright object when viewed from Earth, its proximity to the Sun makes it more difficult to see than Venus. Two spacecraft have visited Mercury: Mariner 10 flew by in the 1970s and MESSENGER, launched in 2004, remains in orbit.";
    private String mythology = "Mercury (/ˈmɜrkjʉri/; Latin: Mercurius About this sound listen (help·info)) is a major Roman god, being one of the Dii Consentes within the ancient Roman pantheon. He is the patron god of financial gain, commerce, eloquence (and thus poetry), messages/communication (including divination), travelers, boundaries, luck, trickery and thieves; he is also the guide of souls to the underworld.[1][2] He was considered the son of Maia and Jupiter in Roman mythology. His name is possibly related to the Latin word merx (merchandise; compare merchant, commerce, etc.), mercari (to trade), and merces (wages); another possible connection is the Proto-Indo-European root merĝ- for boundary, boarder (cf. Old English mearc, Old Norse mark, Latin margō, and Welsh Cymro) and Greek οὖρος (by analogy of Arctūrus/Ἀρκτοῦρος), as the keeper of boundaries, referring to his role as bridge between the upper and lower worlds.[citation needed] In his earliest forms, he appears to have been related to the Etruscan deity Turms, both of which share characteristics with the Greek god Hermes. In Virgil's Aeneid, Mercury reminds Aeneas of his mission to found the city of Rome. In Ovid's Fasti, Mercury is assigned to escort the nymph Larunda to the underworld. Mercury, however, fell in love with Larunda and made love to her on the way. Larunda thereby became mother to two children, referred to as the Lares, invisible household gods.Mercury has influenced the name of many things in a variety of scientific fields, such as the planet Mercury, and the element mercury. The word mercurial is commonly used to refer to something or someone erratic, volatile or unstable, derived from Mercury's swift flights from place to place. He is often depicted holding the caduceus in his left hand.";
    private String match = "system";
    @Test
    public void selectMatch() throws UnsupportedException, ExecutionException, com.stratio.connector.meta.exception.UnsupportedOperationException {
    	
    	
    	insertRow(M_ELEM, 100025,element);
        insertRow(M_PLANET ,1000000,planet);
        insertRow(M_MYTHO,  100,mythology);
        insertRow(M_FRED,  50000,freddie);

        
        refresh();

        
        LogicalPlan logicalPlan = null;
        List<LogicalStep> stepList = new ArrayList<>();
        List<ColumnMetadata> columns = new ArrayList<>();

        columns.add(new ColumnMetadata(COLLECTION,COLUMN_NAME));
        columns.add(new ColumnMetadata(COLLECTION,COLUMN_NUMVIEWS));
        Project project = new Project(CATALOG, COLLECTION,columns);
        stepList.add(project);
        
        Match query = new Match(COLUMN_TEXT, 0, true);
        query.addTerms("system","planets","solar","latin");
        query.setMinimunMatch(1); //planet, mythology .. 3 => planet...
        stepList.add(query);
        
        
        Limit limit = new Limit(5);
        stepList.add(limit);
        
        logicalPlan = new LogicalPlan(stepList);
  
        //TODO no sorting by default ?? sort by default
        
        
        QueryResult queryResult = (QueryResult) ((ElasticsearchQueryEngine) stratioElasticConnector.getQueryEngine()).execute(logicalPlan);
       
        List<String> proveSet = new ArrayList<>();
        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
        while(rowIterator.hasNext()){
        	Row row = rowIterator.next();
            for (String cell:row.getCells().keySet()){
            	proveSet.add(cell+row.getCell(cell).getValue());
            	System.out.println(cell+": "+row.getCell(cell).getValue());
            }
        }

        assertEquals("record number",4,proveSet.size());

        assertTrue("Return correct record",proveSet.contains(COLUMN_NAME+M_PLANET));
        assertTrue("Return correct record",proveSet.contains(COLUMN_NUMVIEWS+1000000));
        assertTrue("Return correct record",proveSet.contains(COLUMN_NAME+M_MYTHO));
        assertTrue("Return correct record",proveSet.contains(COLUMN_NUMVIEWS+"100"));




    }




    @Test
    public void selectMatchAndFilter() throws UnsupportedException, com.stratio.connector.meta.exception.UnsupportedOperationException, ExecutionException {




    }


   
    @Test
    public void selectMatchAndSort() throws UnsupportedException,  com.stratio.connector.meta.exception.UnsupportedOperationException, ExecutionException {


    	insertRow(M_ELEM,	 100025,element);
        insertRow(M_PLANET,	 1000000,planet);
        insertRow(M_PLANET+"2",	 10,planet);
        insertRow(M_MYTHO,	 100,mythology);
        insertRow(M_FRED,	 50000,freddie);

        refresh();

        
        LogicalPlan logicalPlan = null;
        List<LogicalStep> stepList = new ArrayList<>();
        List<ColumnMetadata> columns = new ArrayList<>();

        columns.add(new ColumnMetadata(COLLECTION,COLUMN_NAME));
        columns.add(new ColumnMetadata(COLLECTION,COLUMN_NUMVIEWS));
        Project project = new Project(CATALOG, COLLECTION,columns);
        stepList.add(project);
        
        Match query = new Match(COLUMN_TEXT, 0, true);
        query.addTerms("system","planet","solar","earth");
        query.setMinimunMatch(1); //planet, mythology .. 3 => planet...
        stepList.add(query);
        
        
        Sort sort = new Sort(Sort.SCORE);
        stepList.add(sort);
        
        Sort sort2 = new Sort(COLUMN_NUMVIEWS, Sort.DESC);
        stepList.add(sort2);
     
        
        Limit limit = new Limit(2);
        stepList.add(limit);
        
        logicalPlan = new LogicalPlan(stepList);
       
        
        //TODO sort by score and field not yet supported 
        QueryResult queryResult = null;
        try{
        	queryResult = (QueryResult) ((ElasticsearchQueryEngine) stratioElasticConnector.getQueryEngine()).execute(logicalPlan);
	    }catch(ExecutionException e){
	    	if (e.getMessage().startsWith("Sort by score and field unsupported")){
	    		return;
	    	}else throw e;
	    }
        List<String> proveSet = new ArrayList<>();
        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
        while(rowIterator.hasNext()){
        	Row row = rowIterator.next();
            for (String cell:row.getCells().keySet()){
            	proveSet.add(cell+row.getCell(cell).getValue());
            	System.out.println(cell+": "+row.getCell(cell).getValue());
            }
        }

        assertEquals("record number",4,proveSet.size());

        assertTrue("Return correct record",proveSet.get(0).equals(COLUMN_NAME+M_PLANET) || proveSet.get(1).equals(COLUMN_NAME+M_PLANET) );
        assertTrue("Return correct record",proveSet.get(0).equals(COLUMN_NUMVIEWS+1000000) || proveSet.get(1).equals(COLUMN_NUMVIEWS+1000000));
        assertTrue("Return correct record",proveSet.get(2).equals(COLUMN_NAME+M_PLANET+"2") || proveSet.get(3).equals(COLUMN_NAME+M_PLANET+"2"));
        assertTrue("Return correct record",proveSet.get(2).equals(COLUMN_NUMVIEWS+10) || proveSet.get(3).equals(COLUMN_NUMVIEWS+10));

    }


        private void insertRow(String name, int numViews, String text) throws UnsupportedOperationException, ExecutionException{
 	
        	Row row = new Row();
            Map<String, Cell> cells = new HashMap<>();
            cells.put(COLUMN_NAME, new Cell(name));
            cells.put(COLUMN_NUMVIEWS, new Cell(numViews));
            cells.put(COLUMN_TEXT, new Cell(text));

            row.setCells(cells);        
            ((ElasticsearchStorageEngine) stratioElasticConnector.getStorageEngine()).insert(CATALOG, COLLECTION, row);
            
        }


}
