## FUNCTIONS

### CONTAINS
Contains function allows to search a text inside a given field.

**contains (field, value, minimumShouldMatch)**

* **field:** Field to be searched in.
* **value:** Text to be searched.
* **minimumShouldMatch:** For multiterm values indicates the number of matches that a document must include in order to be retreived as a result for the query. It can be expressed either as an absolute number of terms or as a percentage (Negative values indicates missing number of terms or percentage from total).

Example:

    SELECT title FROM movie WHERE contains("title.english", "lord rings", "100%");


### FUZZY
Fuzzy function uses distance algortihms to find results including terms similar to those indicated.   

**fuzzy (field, value, fuzziness)**

* **field:** Field to be searched in.
* **value:** Text to be searched.
* **fuzzines:** Number indicating the maximum [edit distance](https://www.elastic.co/guide/en/elasticsearch/reference/current/common-options.html#fuzziness) to consider a term similar to another.

Example:

    SELECT title FROM movie WHERE fuzzy("title.english", "lorz", "0.6");

### 
MATCH PHRASE
Match Phrase function searches multiterm values as a phrase, meaning that terms must be found in the exact same order to be considered as a match.

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


### MULTI MATCH
Multi Match function is equivalent to the Contains function, but allows to search the given terms inside multiple fields. Those fields can be configured individually using the notation "field^boost" in order to boost those documents where the searched terms appear in the fields with higher boosting values.

**multi\_match (fields..., value, minimumShouldMatch)**

* **field:** Fields to be searched in.
* **value:** Text to be searched.
* **minimumShouldMatch:** Number of matches that a document must include in order to be retreived as a result for the query (Equivalent to the same parameter form Contains function).

Example:

    SELECT title FROM movie WHERE multi_match("title.english", "title^10", "lords", "100%");


### FUZZY MULTI MATCH
Multi Match function is equivalent to the fuzzy function, but allows to search the given terms inside multiple fields. Those fields can be configured individually using the notation "field^boost" in order to boost those documents where the searched terms appear in the fields with higher boosting values.

**multi\_match\_fuzzy (fields..., value, fuzziness)**

* **field:** Field to be searched in.
* **value:** Text to be searched.
* **fuzzines:** Number indicating the maximum edit distance to consider a term similar to another (Equivalent to the same parameter form Fuzzy function).

Example:

    SELECT title FROM movie WHERE multi_match_fuzzy("title.english", "title^10", "lorzs", "0.6");