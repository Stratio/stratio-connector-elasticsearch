## FUNCTIONS

### CONTAINS
Contains function allows to search a text inside one or more fields.

**contains (fields, value, minimumShouldMatch)**

* **fields:** Field or fields to be searched in. Multiple fields can be specified separated by whitespaces. Those fields can be 
configured individually using the notation "field^boost" in order to boost those documents where the searched terms appear 
in the fields with higher boosting values.
* **value:** Text to be searched.
* **minimumShouldMatch:** For multiterm values indicates the number of matches that a document must include in order to be 
retrieved as a result for the query. It can be expressed either as an absolute number of terms or as a percentage (Negative 
values indicates missing number of terms or percentage from total).

Example:

    SELECT title FROM movie WHERE contains("title.english", "lord rings", "100%");
    SELECT title FROM movie WHERE contains("title.english title^10", "lords", "100%");


### FUZZY
Fuzzy function uses distance algorithms to find results including terms similar to those indicated.   

**fuzzy (fields, value, fuzziness)**

* **fields:** Field or fields to be searched in (Equivalent to the "fields" parameter in contains function).
* **value:** Text to be searched.
* **fuzzines:** Number indicating the maximum [edit distance]
(https://www.elastic.co/guide/en/elasticsearch/reference/current/common-options.html#fuzziness) to consider a term similar to another.

Example:

    SELECT title FROM movie WHERE fuzzy("title.english", "lorz", "0.6");
    SELECT title FROM movie WHERE multi_match_fuzzy("title.english title^10", "lorzs", "0.6");

### MATCH PHRASE
Match Phrase function searches multiterm values as a phrase, meaning that terms must be found in the exact same order to be 
considered as a match.

**match\_phrase (field, phrase)**

* **field:** Field to be searched in.
* **phrase:** Text to be searched.

Example:

    SELECT title FROM movie WHERE match_phrase("title", "lord of the rings");


### MATCH PREFIX
Match Prefix function searches for documents including terms starting for the given prefix.

**match\_prefix (field, prefix)**

* **field:** Field to be searched in.
* **prefix:** Prefix to be searched.

Example:

    SELECT title FROM movie WHERE match_prefix("title", "lor");
    