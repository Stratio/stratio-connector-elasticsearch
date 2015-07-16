##EXAMPLES

###Retrieving every document:

    SELECT * FROM movie;

###Counting results:

    SELECT count(*) FROM movie;

###Retrieving specific fields:

    SELECT title,metascore FROM movie;

###Retrieving specific subfield:

    SELECT SUB_FIELD(title,"raw") AS title_raw FROM movie; 
*Though during operations the different analyzers cause different behaviours the results always show the original input, therefore it would make no difference if a field or any subfield is retrieved instead via SELECT, the result would be the same. If using subfields in the select clause it is recommended to assign an alias for the sake of readability.*

###Limiting number of retrieved documents:

    SELECT title FROM movie LIMIT 5;

###Sorting results:

    SELECT title,metascore FROM movie ORDER BY metascore DESC;
    SELECT title,metascore FROM movie ORDER BY metascore ASC;

###Sorting results by subfield:

    SELECT title FROM movie ORDER BY sub_field(title, "normalized") DESC;

###Querying data:

**Searching** (Calculates relevance of results): 

    SELECT title FROM movie WHERE contains("title.english", "lord ring", "100%");

**Filtering** (Does not calculate relevance): 

    SELECT title,year FROM movie WHERE year=2000;

###Grouping by field:

    SELECT rated, count(*) FROM movie GROUP BY rated;

###Grouping by subfield: 

    SELECT sub_field(genre,"raw") AS genre, count(*) FROM movie GROUP BY sub_field(genre,"raw");
*This is the only case where it does matter using a subfield as select instead of the original field, as the select field must be the same as the one used in the group by clause and the results will be displayed transformed by the analyzer.*

###Nested grouping:

    SELECT sub_field(genre,"raw") AS genre, rated, count(*) FROM movie GROUP BY sub_field(genre,"raw"), rated;


###Other examples:

**Documents of type "movie" their year greater or equal to 1950:** 

    SELECT title,year FROM movie WHERE type = "movie" and year>=1950 and year<=2010;

**Ordering the results by year in descending order:** 

    SELECT title,year FROM movie WHERE type = "movie" and year>=1950 and year<=2010 ORDER BY year DESC;

**Limiting the results to those including the word "lord" in their title:** 

    SELECT title,year FROM movie WHERE contains("title.english", "lord", "100%") and type = "movie" and year>=1950 and year<=2010 ORDER BY year DESC;

**Searching "lord" in both title and plot:** 

    SELECT title,year FROM movie WHERE multi_match("title.english", "plot.english", "lord", "100%") and type = "movie" and year>=1950 and year<=2010 ORDER BY year DESC;

**Searching actors similar to the term "elija":** 

    SELECT title,year,actors FROM movie WHERE multi_match("title.english", "plot.english", "lord", "100%") AND fuzzy("actors","elija","0.6") and type = "movie" and year>=1950 and year<=2010 ORDER BY year DESC;

**Searching for both "lord" or "sun" in title and plot instead of just "lord":** 

    SELECT title,year,actors FROM movie WHERE multi_match("title.english", "plot.english", "lord sun", "50%") AND fuzzy("actors","elija","0.6") and type = "movie" and year>=1950 and year<=2015 ORDER BY year DESC;