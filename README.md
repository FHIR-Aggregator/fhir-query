# fhir-query
Leveraging FHIR GraphDefinition for Data Traversals and Local Analysis


---

## Overview  

This project leverages **[FHIR GraphDefinition](https://hl7.org/fhir/graphdefinition.html)** objects to define and execute graph-based traversals across multiple interconnected FHIR resource graphs. The data retrieved is written to a **local SQLite database** for persistence and later transformed into **analyst-friendly dataframes** for analysis using tools like Python’s pandas library.

---

## Motivation  

FHIR Search provides a robust querying framework but comes with significant limitations:  

1. **Deep Chaining Limits**:  
   Chaining searches (e.g., `Patient -> Observation -> Encounter -> Procedure`) often hits server depth limitations.  

2. **Inefficient Query Execution**:  
   Searching deeply related resources requires multiple chained requests, leading to performance issues and unnecessary round trips.  

3. **Lack of Explicit Traversals**:  
   Relationships in FHIR are implicit in references (e.g., `Observation.subject` pointing to `Patient`). This implicit structure requires manual composition of queries, which is prone to errors.  

By using **FHIR GraphDefinition**, we declaratively define resource relationships and efficiently retrieve data. Once retrieved, the data is stored locally and can be transformed into dataframes for advanced analysis.

---

## Key Features  

- **GraphDefinition-Driven Traversals**: Use GraphDefinition objects to define explicit relationships between resources and automate traversal logic.  
- **Local SQLite Storage**: Persist the retrieved FHIR data in a local SQLite database for querying and offline analysis.  
- **Analyst-Friendly Dataframes**: Convert stored FHIR resources into pandas dataframes for ease of use in analytical workflows.  
- **Reusable Graph Definitions**: Maintain a library of GraphDefinition YAML files that can be reused across different workflows and projects.  

---

## Architecture  

### Components  

1. **GraphDefinition Library**  
   - A collection of reusable [GraphDefinition](https://www.hl7.org/fhir/graphdefinition.html) objects in **JSON/YAML** format. A GraphDefinition defines a traversal path between resources.  
   - See [Example GraphDefinition](tests/fixtures/GraphDefinition.yaml), [FHIR Devdays 2021](https://www.devdays.com/wp-content/uploads/2021/12/Rene-Spronk-GraphDefinition-_-DevDays-2019-Amsterdam-1.pdf)

2. **Traversal Engine**  
   - Reads a **GraphDefinition** and iteratively queries the FHIR server using RESTful `_include` and `_revinclude` operations for efficiency.  
   - Stores the retrieved resources in a **SQLite database** in JSON format for flexibility.  

3. **SQLite Data Storage**  
   - Table Schema:  see fhir_query.ResourceDB
 
4. **Analyst-Friendly DataFrames**   **TODO** 
   - Transforms FHIR data from SQLite into pandas dataframes for easier analysis.  
   - Data can be filtered, aggregated, or visualized to meet analytical use cases.  

---

## Workflow  

1. **Load a GraphDefinition**  
   - Define a GraphDefinition object (e.g., `study-to-documents`) to specify the traversal path.  

2. **Execute Traversal**  
   - Use the `Traversal Engine` to query the FHIR server based on the GraphDefinition.  
   - Follow each link and include related resources efficiently using `_include` or `_revinclude`.  

3. **Store Data Locally**  
   - Write the retrieved resources to the SQLite database with their resource types and full JSON representation.  

4. **Transform to DataFrames**  **TODO**
   - Retrieve specific resource types or relationships from the SQLite database.  
   - Convert the JSON data into structured pandas dataframes for analysis.  

---

## Usage

To use the `fq` command, you need to provide the necessary options. Below is an example of how to use the command:
```sh

fq --help
Usage: fq [OPTIONS] COMMAND [ARGS]...

  FHIR-Aggregator utilities.

Commands:
  ls          List all the installed GraphDefinitions.
  run         Run GraphDefinition queries.
  results     Work with the results of a GraphDefinition query.
  vocabulary  FHIR-Aggregator's key Resources and CodeSystems.


```

Examples:

* See [Vocabulary](https://colab.research.google.com/drive/1M2HkLxK_93jvOwPL8iU6te8s9TVne-r1?usp=sharing)
* See [Patient SurvivalCurves](https://colab.research.google.com/drive/1g9EaDNFvlfpKfCQNakCClQxNKQ-kyc6Z?usp=sharing)
* See [GraphDefinitions](https://colab.research.google.com/drive/1G1c_2gNNUdicFWeImN2_zFAjmSwfewYI?usp=drive_link)
