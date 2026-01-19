# Druid Extension: Exact Percentile Using Reservoir Sampling

This extension provides functionalities to calculate exact percentiles using reservoir sampling. 
It introduces new aggregation and SQL capabilities to support percentile computations with configurable reservoir sizes.

## Features

1. **Reservoir Sampling Aggregator**
    - Aggregate a stream of double values into a fixed-size reservoir using `doublesReservoir`.
    - The reservoir size can be configured, ensuring memory efficiency while maintaining accuracy.

2. **Percentile Computation**
    - Compute single or multiple percentiles from a `doublesReservoir` using:
        - `doublesReservoirToPercentile` (for a single percentile).
        - `doublesReservoirToPercentiles` (for multiple percentiles).

3. **SQL Support**
    - SQL counterparts for aggregation and percentile computation:
        - `DR_PERCENTILE_AGG`: Aggregate values into a reservoir.
        - `DR_GET_PERCENTILE`: Retrieve a single percentile from a reservoir.
        - `DR_GET_PERCENTILES`: Retrieve multiple percentiles from a reservoir.

---

## Usage

### Configuration

Before using the extension, ensure it is properly registered and loaded in your Druid cluster. 
Add the extension to your classpath and update the `druid.extensions.loadList` in your Druid configuration to include this extension.

### Aggregator Usage in Ingestion

Define an aggregator in your ingestion specification to aggregate values into a reservoir.

```json
{
  "type": "doublesReservoir",
  "name": "sample_reservoir",
  "fieldName": "metric_column",
  "maxReservoirSize": 1000
}
```

#### Parameter Descriptions
* `type`: Specifies the aggregator type (`doublesReservoir`).
* `name`: Name of the resulting metric.
* `fieldName`: Input column containing double values.
* `maxReservoirSize`: Configurable size of the reservoir.

---

### Post-Aggregation for Percentiles

After aggregating your data into a reservoir using the `doublesReservoir` aggregator, you can compute percentiles using two post-aggregator types:
1. **Single Percentile (`doublesReservoirToPercentile`)**
2. **Multiple Percentiles (`doublesReservoirToPercentiles`)** 
 
These post-aggregators operate on the reservoir, which contains the sampled data, and compute the desired percentiles.

### 1. Single Percentile

To compute a single percentile from the reservoir, use the `doublesReservoirToPercentile` post-aggregator. 
This will calculate the percentile value at a specific fraction of the dataset.

For example, to compute the 90th percentile from the `sample_reservoir`:

```json
{
  "type": "doublesReservoirToPercentile",
  "name": "90th_percentile",
  "field": {
      "type": "fieldAccess",
      "fieldName": "sample_reservoir"
   },
  "fraction": 0.9
}
```
#### Parameter Descriptions
* `type`: Specifies the post-aggregator type (`doublesReservoirToPercentile`).
* `name`: The name of the output field that will hold the percentile value (e.g., `90th_percentile`).
* `field`: The name of the `PostAggregator` that produces the reservoir data
* `fraction`: The desired percentile expressed as a fraction of the dataset. For example:
  *	0.9 for the 90th percentile.
  *	0.5 for the median (50th percentile).
  *	0.99 for the 99th percentile.

The result will be a single value representing the percentile from the reservoir data.


### 2. Multiple Percentiles

To compute multiple percentiles at once, use the `doublesReservoirToPercentiles` post-aggregator. 
This allows you to retrieve several percentiles from the same reservoir, in a single operation.

For example, to compute the 50th, 90th, and 99th percentiles:

```json
{
  "type": "doublesReservoirToPercentiles",
  "name": "percentiles", 
  "field": {
      "type": "fieldAccess",
      "fieldName": "sample_reservoir"
   },
  "fractions": [0.5, 0.9, 0.99]
}
```

#### Parameter Descriptions
* `type`: Specifies the post-aggregator type (`doublesReservoirToPercentiles`).
* `name`: The name of the output field that will hold the percentile value (e.g., `percentiles`).
* `field`: The name of the `PostAggregator` that produces the reservoir data
* `fractions`: An array of fractions representing the percentiles you want to calculate. For example:
  *	`[0.5, 0.9, 0.99]` calculates the `50th`, `90th`, and `99th` percentiles.
  *	You can include any number of fractions depending on your needs.

The result will be an array of percentile values, one for each fraction specified in the fractions array.

### Summary of Differences

| **Operation**             | **Post-Aggregator Type**         | **Result**                                | **Configuration Example**             |
|---------------------------|----------------------------------|-------------------------------------------|---------------------------------------|
| **Single Percentile**     | `doublesReservoirToPercentile`   | A single percentile value for a fraction. | `"fraction": 0.9` (90th percentile)   |
| **Multiple Percentiles**  | `doublesReservoirToPercentiles`  | An array of percentile values.            | `"fractions": [0.5, 0.9, 0.99]`       |

- **Single Percentile**: Computes one percentile (e.g., 90th percentile) based on the specified fraction.
- **Multiple Percentiles**: Computes multiple percentiles (e.g., 50th, 90th, and 99th) in a single step using an array of fractions.

**Example**

```json
{
   "queryType": "timeseries",
   "dataSource": "wikipedia",
   "intervals": [
      "2014-01-01T00:00:00.000Z/2016-01-02T00:00:00.000Z"
   ],
   "granularity": "day",
   "aggregations": [
      {
         "type": "doublesReservoir",
         "name": "reservoir:agg",
         "fieldName": "delta",
         "maxReservoirSize": 1000
      }
   ],
   "postAggregations": [
      {
         "type": "doublesReservoirToPercentiles",
         "name": "AllPercentiles",
         "field": {
            "type": "fieldAccess",
            "fieldName": "reservoir:agg"
         },
         "fractions": [
            "0.1",
            "0.2",
            "0.3",
            "0.4",
            "0.5",
            "0.6",
            "0.7",
            "0.8",
            "0.9",
            "1"
         ]
      }
   ]
}
```
---

### SQL Usage

his extension also supports SQL queries to compute percentiles directly on aggregated data in Druid. 
The following SQL functions are available:

1. **`DR_PERCENTILE_AGG`**: Aggregates values into a reservoir.
2. **`DR_GET_PERCENTILE`**: Retrieves a single percentile from a reservoir.
3. **`DR_GET_PERCENTILES`**: Retrieves multiple percentiles from a reservoir.

### 1. Aggregating Values into a Reservoir (`DR_PERCENTILE_AGG`)

Use the `DR_PERCENTILE_AGG` function in your SQL query to aggregate values into a reservoir. 
This is similar to the `doublesReservoir` aggregator used in the ingestion spec.

#### Syntax: 
```sql
DR_PERCENTILE_AGG(column_name, reservoir_size)
```
#### Parameter Descriptions
* `column_name`: The name of the column containing the data you want to aggregate.
* `reservoir_size`: The size of the reservoir. This determines how many elements are sampled.

**Example**

```sql
SELECT 
  DR_PERCENTILE_AGG(response_time, 1000) AS response_time_reservoir
FROM datasource
```
This query aggregates the response_time values into a reservoir of size `1000`

### 2.  Retrieving a Single Percentile (`DR_GET_PERCENTILE`)

After aggregating values into a reservoir, you can retrieve a single percentile using the `DR_GET_PERCENTILE` function. 
The function computes the percentile from the reservoir based on the specified fraction.

#### Syntax:
```sql
DR_GET_PERCENTILE(reservoir_name, percentile_fraction)
```
#### Parameter Descriptions
* `reservoir_name`: The name of the aggregated reservoir (as generated by `DR_PERCENTILE_AGG`).
* `percentile_fraction`: A fraction representing the percentile you want to retrieve. For example, `0.9` for the `90th` percentile.

**Example**

```sql
SELECT DR_GET_PERCENTILE(reservoir, 0.9) AS percentile_90
FROM (
  SELECT DR_PERCENTILE_AGG(response_time_reservoir, 20) AS reservoir
  FROM "datasource"
  WHERE __time BETWEEN TIMESTAMP '2014-11-01 00:00:00' AND TIMESTAMP '2016-11-30 23:59:59'
)
```

This query retrieves the 90th percentile from the response_time_reservoir.

### 3. Retrieving Multiple Percentiles (`DR_GET_PERCENTILES`)
To retrieve multiple percentiles at once, use the `DR_GET_PERCENTILES` function. 
This function allows you to pass an array of fractions representing the percentiles you want to calculate.

#### Syntax:
```sql
DR_GET_PERCENTILES(reservoir_name, percentile_fractions)
```
#### Parameter Descriptions
* `reservoir_name`: The name of the aggregated reservoir (as generated by `DR_PERCENTILE_AGG`).
* `percentile_fractions`: A comma separated list of fractions representing the percentiles you want to calculate. 
For example, `0.5, 0.9, 0.99` for the 50th, 90th, and 99th percentiles.

**Example**

```sql
SELECT DR_GET_PERCENTILES(reservoir, 0.5, 0.9, 0.99) AS percentiles
FROM (
  SELECT DR_PERCENTILE_AGG(response_time_reservoir, 20) AS reservoir
  FROM "datasource"
  WHERE __time BETWEEN TIMESTAMP '2014-11-01 00:00:00' AND TIMESTAMP '2016-11-30 23:59:59'
)
```
This query retrieves an array of percentiles (50th, 90th, and 99th) from the response_time_reservoir.

### Summary of SQL Functions

| **SQL Function**         | **Description**                                              | **Syntax Example**                                            |
|--------------------------|--------------------------------------------------------------|---------------------------------------------------------------|
| **`DR_PERCENTILE_AGG`**  | Aggregates values into a reservoir with the specified size.  | `DR_PERCENTILE_AGG(response_time, 1000)`                      |
| **`DR_GET_PERCENTILE`**  | Retrieves a single percentile value from the reservoir.      | `DR_GET_PERCENTILE(response_time_reservoir, 0.9)`             |
| **`DR_GET_PERCENTILES`** | Retrieves a list of percentiles from the reservoir.          | `DR_GET_PERCENTILES(response_time_reservoir, 0.5, 0.9, 0.99)` |

These SQL functions provide a flexible and efficient way to compute percentiles directly in Druid using SQL queries. 
They enable you to aggregate data, calculate percentiles, and analyze the data without needing to write complex Java code.
---

### Other Calculations
Besides percentiles, the following calculations can be performed from a `doublesReservoir` field (others can be added in future releases):

#### Sample Standard Deviation
- Post-Aggregation: To compute sample standard deviation from the reservoir, use the `doublesReservoirToStddev` post-aggregator.
  For example, to compute the standard deviation from the `sample_reservoir`:

```json
{
  "type": "doublesReservoirToStddev",
  "name": "sample_stddev",
  "field": {
      "type": "fieldAccess",
      "fieldName": "sample_reservoir"
   }
}
```

- SQL Usage: After aggregating values into a reservoir, you can retrieve the sample standard deviation using the `DR_GET_STDDEV` function.
```sql
DR_GET_STDDEV(reservoir_name)
```

#### Sample AVG
- Post-Aggregation: To compute sample AVG from the reservoir, use the `doublesReservoirToAVG` post-aggregator.
  For example, to compute the mean from the `sample_reservoir`:

```json
{
  "type": "doublesReservoirToAVG",
  "name": "sample_avg",
  "field": {
      "type": "fieldAccess",
      "fieldName": "sample_reservoir"
   }
}
```
---
#### Sample MAX
- Post-Aggregation: To compute the sample MAX from the reservoir, use the `doublesReservoirToMAX` post-aggregator.  
  This post-aggregator extracts the maximum value from a `double` reservoir that was previously
  populated during aggregation:


- SQL Usage: After aggregating values into a reservoir, you can retrieve the sample mean using the `DR_GET_AVG` function.
```sql
DR_GET_MAX(reservoir_name)
```
---
#### Sample MIN
- Post-Aggregation: To compute the sample MIN from the reservoir, use the `doublesReservoirToMIN` post-aggregator.  
This post-aggregator extracts the minimum value from a `double` reservoir that was previously
populated during aggregation.
- - SQL Usage: After aggregating values into a reservoir, you can retrieve the sample mean using the `DR_GET_AVG` function.
```sql
DR_GET_MIN(reservoir_name)
```

---
### Build

To build the extension, run `mvn package` and you'll get a file in `target` directory.
Unpack the `tar.gz`.

```
$ tar xzf target/druid-exact-percentile-30.0.1-bin.tar.gz
$ ls druid-exact-percentile/
LICENSE                  README.md               ddruid-exact-percentile-30.0.1-bin.tar.gz
```

---

### Install

To install the extension:

1. Copy `druid-exact-percentile` into your Druid `extensions` directory.
2. Edit `conf/_common/common.runtime.properties` to add `druid-exact-percentile` to `druid.extensions.loadList`. (
   Edit `conf-quickstart/_common/common.runtime.properties` too if you are using the quickstart config.)
   It should look like: `druid.extensions.loadList=["druid-exact-percentile"]`. There may be a few other extensions
   there too.
3. Restart Druid.