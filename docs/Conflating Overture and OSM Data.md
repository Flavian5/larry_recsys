# **The Convergence of Discrete Global Grid Systems and Open Map Data: A Technical Analysis of H3, S2, and Geohash in the Conflation of Overture and OpenStreetMap**

The modern geospatial engineering landscape is defined by the tension between the exponential growth of location-based data and the inherent limitations of traditional Euclidean geometry for large-scale analysis. As organizations attempt to synthesize the community-curated richness of OpenStreetMap with the high-precision, multi-sourced outputs of the Overture Maps Foundation, the challenge of conflation—identifying and merging duplicate features into a non-redundant, authoritative set—has become the central problem of the domain.1 Traditional spatial joins, while mathematically precise, lack the computational efficiency required to operate on datasets containing billions of features, such as building footprints or road segments.2 In response, the field has gravitated toward Discrete Global Grid Systems (DGGS), specifically Geohash, Google’s S2, and Uber’s H3, which provide a discretized, hierarchical scaffolding for the Earth's surface.2 These systems enable a fundamental shift in geospatial architecture: the transition from complex geometric intersections to simple, high-velocity alphanumeric joins.5

## **The Historical Evolution of Global Spatial Indexing**

The history of spatial indexing is a trajectory from local, planar representations toward global, robust, and seam-free spherical frameworks. Early geographic information systems (GIS) relied on projected coordinate systems, which were inherently limited by the distortions and singularities associated with mapping a three-dimensional ellipsoid onto a two-dimensional plane.8 The U.S. Census Bureau’s TIGER (Topologically Integrated Geographic Encoding and Referencing) system represents an early milestone in this evolution, establishing a framework for topological relationships in national datasets.9 However, as the scale of data shifted from national to global, the need for a unified, partitionable indexing system became critical.

### **The Emergence of Geohash (2008)**

In 2008, Gustavo Niemeyer introduced Geohash, a public-domain system that encodes latitude and longitude into a short alphanumeric string.4 Geohash represented a significant breakthrough in the "geofencing" and location-based search era of web development.11 The mechanism involves a recursive bisection of the Earth’s surface into a rectangular grid.4 For each bit of the hash, the algorithm alternates between bisecting longitude and latitude, assigning a 0 or 1 based on which half of the current bounding box the point occupies.4 These bits are then interleaved and converted into a Base32 representation.4

Geohash’s primary innovation was its prefix-based hierarchy: features located within the same string prefix are generally spatially proximal.4 This allowed for efficient range queries in databases that were not natively spatial.4 However, the system's reliance on an equirectangular projection introduced severe distortions near the poles, where the width of a cell approaches zero.4 Furthermore, Geohash utilizes a Z-order (Morton) space-filling curve, which exhibits significant discontinuities at the boundaries of its quadrants—two points can be centimeters apart but have entirely different Geohash strings.4

### **Google S2 and the Hilbert Curve (2011)**

To address the limitations of Geohash, particularly for global-scale operations like Google Maps, Google developed the S2 geometry library.8 S2, released as open-source in 2011, moved beyond rectangular grids by projecting the Earth onto an unfolded cube.12 Each of the six faces of the cube is subdivided into a quadtree of quadrilateral cells.12 The critical advancement in S2 was the adoption of the Hilbert space-filling curve, a fractal structure that preserves spatial locality much more effectively than the Morton curve.4

In the S2 system, the Hilbert curve traverses the subdivided cube faces in a U-shaped pattern, ensuring that points close in physical space are highly likely to have numerically close 64-bit integer identifiers.4 This property is essential for the distributed spatial indexing required by massive search engines.13 S2 provides 31 levels of resolution, with level 0 representing the entire cube face and level 30 providing a precision of approximately 1 centimeter.13 Because S2 cells are quadrilaterals, they maintain a strict "aperture 4" hierarchy: one parent cell is composed of exactly four child cells, a property that simplifies backend containment and "point-in-polygon" logic.12

### **Uber H3 and the Hexagonal Shift (2018)**

Uber introduced the H3 system in 2018 to solve the specific problems of city-scale mobility and dynamic ride-sharing.16 Uber’s data scientists found that square grids introduced "aliasing" artifacts in neighborhood analysis and flow modeling.15 H3 utilizes a hexagonal grid, which offers a unique mathematical property: the distance between a hexagon’s center and the centers of all its six neighbors is identical.4 This symmetry makes hexagons the superior choice for neighbor traversal, gradient analysis, and data smoothing.2

H3 utilizes an icosahedron projection, mapping the sphere onto 20 triangular faces.4 This projection further reduces the spatial distortion found in the equirectangular and cube projections of Geohash and S2.4 However, tiling a sphere with hexagons is geometrically impossible; H3 compensates by placing 12 pentagons at the vertices of the icosahedron.4 H3 employs an "aperture 7" subdivision, where one parent cell is approximately covered by seven child cells.12 Unlike S2, H3 does not maintain a strict geometric hierarchy; a child hexagon may not be perfectly contained within the boundary of its parent hexagon, leading to "logical" rather than "geometric" containment.15

## **Technical Foundations of Discrete Global Grid Systems**

The selection of a spatial indexing system is a trade-off between mathematical robustness, hierarchical stability, and analytical utility. Each system provides a unique mechanism for "discretizing" the continuous surface of the Earth into a finite number of buckets that can be processed as integers or strings.

### **Projection and Distortion Metrics**

The choice of projection significantly influences the uniformity of cell area across the globe. This is critical for statistical analysis, where comparing counts in one region to another requires that the underlying units of area are equivalent.

| Metric | Geohash | Google S2 | Uber H3 |
| :---- | :---- | :---- | :---- |
| **Projection** | Equirectangular | Unfolded Cube | Icosahedron |
| **Space-Filling Curve** | Z-Order (Morton) | Hilbert | None (Global ID based) |
| **Area Uniformity** | Poor (Poles) | Good | Excellent |
| **Hierarchy** | Strict (Prefix) | Strict (Aperture 4\) | Approximate (Aperture 7\) |
| **ID Format** | Base32 String | 64-bit Integer | 64-bit Integer |
| **Primary Goal** | Search/Geofencing | Backend Lookup | Visualization/Analytics |

2

In Geohash, the cell width varies with the cosine of the latitude, meaning a Geohash at the equator has a vastly different area than one at the North Pole.4 S2’s cube projection utilizes transformation functions (quadratic or tangential) to minimize the area distortion within each cube face.8 H3’s icosahedral approach achieves the highest level of uniformity by having more faces than a cube, thus approximating the sphere more closely.4 For users requiring even stricter area consistency, the A5 system (Pentagons) has recently emerged, though it lacks the broad community and database support of H3 or S2.2

### **Space-Filling Curves and Locality**

Space-filling curves map two-dimensional coordinates to a one-dimensional sequence while attempting to preserve spatial proximity.4 This is a form of locality-sensitive hashing.4

* **Z-Order Curve (Geohash):** Constructed by interleaving the bits of the X and Y coordinates.4 While simple to compute, it suffers from "jumps" where the curve must travel across the entire space to reach the next quadrant, resulting in points that are physically adjacent but numerically distant.4  
* **Hilbert Curve (S2):** A more complex fractal that maintains better locality.4 The Hilbert curve "snakes" through the grid in a way that minimizes the distance between consecutive points in the sequence.4 This property is leveraged by databases to perform range scans that effectively capture regional data in a single I/O operation.4

H3 does not globally follow a space-filling curve in the same manner as S2.4 Instead, it uses a unique 64-bit identifier that encodes the resolution, the base cell (one of 122), and the child indices.6 This structure allows H3 to excel in neighbor traversal—calculating the neighbors of a cell is a constant-time bitwise operation rather than a spatial query.6

## **The Overture Maps Foundation and GERS**

The Overture Maps Foundation represents a massive effort by AWS, Meta, Microsoft, and TomTom to consolidate global map data into a single, interoperable schema.9 The project’s central goal is to provide a "market-grade" alternative to proprietary maps by conflating OpenStreetMap with other high-quality sources, including government records and machine-learning-derived building footprints.20

### **The Global Entity Reference System (GERS)**

Interoperability in the Overture ecosystem is driven by the Global Entity Reference System (GERS).22 GERS provides a persistent, unique ID for every feature, such as a building, a place, or a road segment.23 A significant limitation of OpenStreetMap is that IDs are not always stable; a building might be deleted and redrawn by a user, resulting in a new ID that breaks existing links to external databases.23 GERS IDs are designed to persist across monthly releases, providing a stable "hook" for data enrichment.9

The internal structure of a GERS ID is a standard UUID (Universally Unique Identifier), ensuring it can be used across any modern database without collision.23 Technically, there is a relationship between GERS and spatial indexing: the first 16 digits of many GERS IDs correspond to an H3 cell ID.26 This allows analysts to perform a "rough" spatial filter simply by inspecting the ID string, enabling the identification of the general geographic region of an entity without loading its full geometry.26

### **Overture’s Conflation Hierarchy**

Overture does not simply dump data from multiple sources; it applies a rigorous conflation workflow to determine the "best" version of a feature.28 For the Buildings theme, for example, the conflation order typically prioritizes OpenStreetMap, followed by Esri Community Maps, and then high-precision machine-learning datasets from Google and Microsoft.29

| Priority | Data Source | Rationale |
| :---- | :---- | :---- |
| **1** | OpenStreetMap | Human-verified, detailed attributes |
| **2** | Esri Community Maps | Authoritative government-verified data |
| **3** | Google Open Buildings (High Precision) | 90%+ confidence ML footprints |
| **4** | Microsoft ML Building Footprints | Broad global coverage |
| **5** | Google Open Buildings (Low Precision) | Filling gaps in unmapped regions |

29

To maintain transparency, Overture provides "Bridge Files" with each release.24 These files are join tables that link a GERS ID to its source identifiers (e.g., an OSM way ID or an Esri structure UUID).23 This allows users to "reverse lookup" an Overture building to find its original OSM record and see exactly which version of the geometry was used.30

## **Database Handling of Spatial Indices at Scale**

The primary utility of H3, S2, and Geohash lies in how they are implemented within modern database architectures. The choice of database—whether transactional (OLTP) or analytical (OLAP)—governs how these indices are utilized.

### **PostGIS: Transactional Integrity and R-Trees**

PostGIS, the spatial extension for PostgreSQL, remains the industry standard for transactional workloads.3 PostGIS utilizes an R-Tree index, which groups geometries into "Minimum Bounding Rectangles" (MBRs) to prune the search space during a query.6 While PostGIS is highly precise, its performance can degrade during massive joins across billions of rows.3

In a conflation workflow, PostGIS developers often use H3 or S2 as a "secondary index".2 By appending an H3 integer column to a table and creating a standard B-tree index on that column, a developer can perform a "coarse" join:

SQL

SELECT \* FROM osm\_buildings o  
JOIN overture\_buildings v ON o.h3\_res9 \= v.h3\_res9  
WHERE ST\_Intersects(o.geom, v.geom);

This hybrid approach leverages the speed of integer matching to reduce the candidates for the more expensive ST\_Intersects operation, often resulting in massive performance gains without sacrificing accuracy.2

### **Google BigQuery: Native S2 Clustering**

Google BigQuery is an analytical data warehouse that natively supports the S2 geometry system.15 When a table is clustered by a GEOGRAPHY column, BigQuery internally partitions and shards the data using S2 cells.32 This means that spatial joins in BigQuery are automatically optimized by the engine without requiring the user to manually calculate grid IDs.15

However, BigQuery’s H3 support is not native; it is typically implemented through Javascript UDFs or the Carto Analytics Toolbox.15 A common pitfall in BigQuery is attempting to use H3 for clustering.32 Because H3\_ToParent operations involve bitwise arithmetic that is too complex for BigQuery’s query optimizer to understand, clustering on an H3 index often results in a full table scan rather than the intended partition pruning.32

### **DuckDB: Local Analytics and GeoParquet**

DuckDB has emerged as a disruptive force in local geospatial analytics.3 As an embedded database, it is optimized for the GeoParquet format used by Overture.34 DuckDB’s spatial extension provides native support for GDAL, allowing it to export DuckDB tables directly to GeoJSON, FlatGeobuf, or GeoPackage.34

For conflation, DuckDB features a community H3 extension that is remarkably efficient.36 Because DuckDB uses a vectorized execution engine—processing data in chunks rather than row-by-row—it can aggregate millions of points into H3 cells on a standard laptop in seconds.3 This allows analysts to perform sophisticated spatial joins and string-similarity matching between OSM and Overture data without the overhead of a server or a cluster.34

### **Apache Sedona: Distributed Spatial Joins**

When the dataset exceeds the capacity of a single machine, Apache Sedona (built on Spark) is the preferred solution.39 Sedona’s primary advantage is "spatial partitioning".41 Unlike standard Spark partitions, which might be based on a hash of a name, Sedona groups data that is geographically close onto the same machine.39

Sedona provides comprehensive H3 support, including functions like ST\_H3CellIDs and ST\_H3KRing.18 In a distributed conflation workflow, Sedona can partition both OSM and Overture data using a common KDB-tree, ensuring that potential matches are always processed within the same Spark task, thereby minimizing the "shuffle" (the costly movement of data across the network).39

## **Conflation Methodology: Strategies for Merging Datasets**

Conflation is the process of reconciling overlapping datasets to create a "golden" record.1 In the context of Overture and OSM, this usually involves matching buildings, road segments, or Points of Interest (POIs).

### **Step 1: Regional Extract and Bounding Box Filtering**

Overture data is distributed globally in Parquet files partitioned by theme (e.g., theme=buildings).30 To avoid downloading the entire global dataset, engineers use a bounding box filter.43 Tools like DuckDB or the Overture Python CLI utilize Parquet "metadata pruning" to download only the chunks of data that intersect the user’s area of interest, significantly reducing network costs.35

### **Step 2: Spatial Bucketing using H3 or S2**

Once the regional data is loaded, each feature is assigned a grid ID at a resolution appropriate for the feature type.2

* **Buildings:** Resolution 9 or 10 is typically used, providing a cell size comparable to a large building footprint.2  
* **POIs/Places:** Resolution 7 or 8 is often used to group candidates within a neighborhood before applying more precise matching.36

This bucketing converts a ![][image1] spatial join (where every point is compared to every polygon) into a ![][image2] hash join on the grid ID.2

### **Step 3: Name and Address Similarity (Fuzzy Matching)**

Spatial proximity alone is rarely enough for high-confidence conflation.36 After identifying candidates in the same H3 bucket, engineers apply string similarity algorithms to attributes like names.primary and address.street.36

| Algorithm | Strengths | Use Case |
| :---- | :---- | :---- |
| **Jaro-Winkler** | Weights prefixes more heavily | Address matching (e.g., "123 Main St" vs "123 Main Street") |
| **Levenshtein** | Counts edits/typos | Name matching with spelling errors |
| **Cosine Similarity** | Compares character n-grams | Catching partial name matches (e.g., "Arco" vs "Bernal Arco") |
| **Embeddings** | Semantic understanding | Advanced matching using vector search (VSS) |

36

A common strategy is to use a similarity threshold (e.g., 0.75 for Jaro-Winkler).36 If two records in the same H3 cell have names that meet this threshold, they are flagged as a potential match.36

### **Step 4: Attribute Merging and GERS Enrichment**

The final stage of conflation is the creation of a unified feature.1 For example, if an OSM building (source of geometry) is matched with an Overture building (source of height and classification), the final record might combine the OSM geometry with the Overture attributes.45 This record is then "GERS-enabled," meaning it is assigned the stable Overture GERS ID, allowing it to be linked to future releases and external datasets (like FEMA insurance data or real-time traffic feeds).25

## **Solving Geospatial Problems at Different Scales**

The architectural approach to conflation must be tailored to the scale of the problem. A "one-size-fits-all" solution often leads to either excessive infrastructure costs or insurmountable local performance bottlenecks.

### **The Laptop Scale: Small to Medium Datasets ( \< 10M Features)**

For city-level analysis or small research projects, the primary constraint is ease of setup and I/O speed.

* **Stack:** DuckDB, GeoParquet, and Python (GeoPandas/H3-py).3  
* **Approach:** Stream data directly from S3 using DuckDB’s httpfs extension.34 Perform H3-based joins in-memory.37  
* **Insight:** DuckDB’s ability to process "larger than memory" files via out-of-core execution allows a standard laptop to handle datasets that previously required a dedicated server.3

### **The Regional/Enterprise Scale: Medium to Large Datasets (10M \- 500M Features)**

For organizations maintaining regional maps or logistics networks, the constraints are transactional integrity and support for multiple users.

* **Stack:** PostgreSQL/PostGIS, DBT (Data Build Tool), and H3-PG extension.3  
* **Approach:** Create a "Medallion" architecture.2 Store raw OSM and Overture data in "Bronze" tables. Use DBT to run periodic conflation scripts that populate "Silver" (normalized) and "Gold" (conflated/GERS-enabled) tables.2  
* **Insight:** PostGIS is excellent for maintaining a "live" map where editors are constantly making changes.3 Using H3 as a secondary index in PostGIS provides the necessary speed for daily updates without migrating to a big data platform.7

### **The Global/Cloud Scale: Massive Datasets ( \> 500M Features)**

For companies like Meta or Microsoft processing the entire planet’s building footprints (2.5B+) and road segments, the constraint is distributed compute and data skew.31

* **Stack:** Apache Sedona, Databricks/Spark, and Snowflake.31  
* **Approach:** Implement a "Spatial Lakehouse".41 Store data in open table formats like Apache Iceberg.41 Use Sedona’s adaptive partitioning to handle dense urban areas (hotspots) differently than sparse rural areas.39  
* **Insight:** At this scale, the cost of "shuffling" data across the network becomes the dominant factor.41 Proper spatial partitioning is more important than the specific indexing algorithm (H3 vs. S2).41 Sedona’s price-performance for massive joins often exceeds that of native cloud SQL warehouses due to its specialized spatial join strategies.47

## **Advanced Insights: The "Tax" of Discretization**

While H3, S2, and Geohash are powerful, they are approximations of reality. This introduction of discretization leads to several second-order consequences that engineers must manage.

### **The Boundary Problem and Buffer Rings**

Features often lie near the edge of a grid cell.4 If an OSM point is in cell ![][image3] and a matching Overture point is in the adjacent cell ![][image4], a simple hash join on the cell ID will fail to find them.4 To solve this, practitioners use "neighbor traversal".6

In H3, the ST\_H3KRing function returns a cell and all its neighbors within distance ![][image5].18 In a high-confidence conflation workflow, an engineer might join an OSM feature not just to its own H3 cell, but to the entire 7-cell ring (the cell plus its 6 immediate neighbors).7 While this increases the number of candidates for fuzzy matching, it eliminates "false negatives" caused by grid boundaries.7

### **Precision vs. Accuracy in Hexagons**

The H3 "aperture 7" hierarchy is logically sound but geometrically imperfect.15 Because child hexagons do not perfectly fill the parent hexagon, there is a "shimmer" in the data when zooming across resolutions.4 For visualization, this is rarely an issue.2 For engineering tasks—such as determining if a high-resolution building footprint "is contained by" a lower-resolution land-use polygon—this approximate hierarchy can lead to errors.17 In such cases, S2’s strict "aperture 4" quadtree structure is often preferred because of its geometric guarantee of containment.12

### **The Future of Open Data Interoperability**

The convergence of Overture and OpenStreetMap through DGGS is moving the industry toward a "Linked Open Data" model for geography.9 In this future, a map is not a single monolith but a series of interconnected layers.9 A GERS ID for a building might link to an OSM geometry, a Microsoft ML classification, a Google address, and a localized energy-efficiency score from a city government.9

The role of H3 and S2 in this ecosystem is to provide the "spatial glue".9 They allow different mapping teams to perform their work independently while ensuring that their outputs can be merged effortlessly through a common grid.2 As AI models (LLMs) begin to integrate spatial reasoning, tools like Sedona and H3 are becoming the "spatial brain" that provides physical context to language-based models, bridging the gap between text generation and real-world spatial intelligence.41

## **Summary of Implementation Recommendations**

For professional practitioners embarking on the conflation of Overture and OSM data, the following strategic principles should guide the technical architecture.

1. **Prioritize the Grid for Speed, Geometry for Accuracy:** Always use a grid system (H3 or S2) to bucket features for candidate selection, but never discard the original raw geometries.2 The final point-in-polygon or intersection test must be performed on the actual shapes to ensure precision.7  
2. **Match the Index to the Goal:** Use H3 for analytics, visualization, and neighborhood-based smoothing.2 Use S2 for backend storage, strict containment queries, and high-performance range scans.2  
3. **Leverage GERS and Bridge Files:** Do not reinvent the wheel.9 Use Overture’s pre-computed Bridge Files to map OSM features to GERS IDs whenever possible.30 This drastically reduces the computational burden of global conflation.23  
4. **Scale with Infrastructure:** Use DuckDB for local exploration and Python-based data science.3 Use PostGIS for transactional map maintenance.3 Use Apache Sedona for global, distributed analytical pipelines.39  
5. **Address the Discontinuities:** Account for grid boundary effects by using neighbor traversal (rings) in H3 or cell-covering expansion in S2.6 Ensure that string-similarity matching is weighted alongside spatial proximity to achieve the highest confidence in conflated records.36

The integration of these disparate global grid systems with the emerging Overture/OSM data ecosystem represents the most significant advance in map-making technology in decades. By mastering the mathematical nuances of H3 and S2 and the architectural patterns of modern geo-databases, engineers can transform the chaotic world of raw geospatial data into a structured, interoperable, and authoritative digital twin of the planet.2

#### **Works cited**

1. Conflation \- OpenStreetMap Wiki, accessed on February 26, 2026, [https://wiki.openstreetmap.org/wiki/Conflation](https://wiki.openstreetmap.org/wiki/Conflation)  
2. Beyond Latitude & Longitude: A Guide to H3, S2, and Discrete ..., accessed on February 26, 2026, [https://forrest.nyc/discrete-global-grid-systems-h3-s2-vs-lat-long/](https://forrest.nyc/discrete-global-grid-systems-h3-s2-vs-lat-long/)  
3. The Spatial SQL Landscape in 2026: A Guide to 50+ Databases \- Matt Forrest, accessed on February 26, 2026, [https://forrest.nyc/best-spatial-sql-tools/](https://forrest.nyc/best-spatial-sql-tools/)  
4. Geospatial Indexing Explained: A Comparison of Geohash, S2, and ..., accessed on February 26, 2026, [https://benfeifke.com/posts/geospatial-indexing-explained/](https://benfeifke.com/posts/geospatial-indexing-explained/)  
5. Geospatial Indexing Sampler, accessed on February 26, 2026, [https://aaronblondeau.com/posts/december\_2024/spatial\_indexes/](https://aaronblondeau.com/posts/december_2024/spatial_indexes/)  
6. Breaking Down Location-Based Algorithms: R-Tree, Geohash, S2, and H3 Explained | by Sylvain Tiset | Medium, accessed on February 26, 2026, [https://medium.com/@sylvain.tiset/breaking-down-location-based-algorithms-r-tree-geohash-s2-and-h3-explained-a65cd10bd3a9](https://medium.com/@sylvain.tiset/breaking-down-location-based-algorithms-r-tree-geohash-s2-and-h3-explained-a65cd10bd3a9)  
7. Fast spatial query db? : r/dataengineering \- Reddit, accessed on February 26, 2026, [https://www.reddit.com/r/dataengineering/comments/1lm7j6u/fast\_spatial\_query\_db/](https://www.reddit.com/r/dataengineering/comments/1lm7j6u/fast_spatial_query_db/)  
8. Overview | S2Geometry, accessed on February 26, 2026, [https://s2geometry.io/about/overview.html](https://s2geometry.io/about/overview.html)  
9. Eliminating the Hidden Tax: How GERS Transforms Geospatial Data Integration \- Linux Foundation, accessed on February 26, 2026, [https://www.linuxfoundation.org/hubfs/Overture%20Maps/Overture\_GERS\_WhitePaper\_Eliminating\_the-Hidden\_Tax-Final.pdf?hsLang=en](https://www.linuxfoundation.org/hubfs/Overture%20Maps/Overture_GERS_WhitePaper_Eliminating_the-Hidden_Tax-Final.pdf?hsLang=en)  
10. TIGER Data Products Guide \- Census Bureau, accessed on February 26, 2026, [https://www.census.gov/programs-surveys/geography/guidance/tiger-data-products-guide.html](https://www.census.gov/programs-surveys/geography/guidance/tiger-data-products-guide.html)  
11. Geohash vs H3: Which Geospatial Indexing System Should I Use? | Data Army Intel, accessed on February 26, 2026, [https://dataarmyintel.io/knowledge-article/geohash-or-h3-which-geospatial-indexing-system-should-i-use/](https://dataarmyintel.io/knowledge-article/geohash-or-h3-which-geospatial-indexing-system-should-i-use/)  
12. S2 | H3, accessed on February 26, 2026, [https://h3geo.org/docs/comparisons/s2/](https://h3geo.org/docs/comparisons/s2/)  
13. S2 Cells | S2Geometry, accessed on February 26, 2026, [https://s2geometry.io/devguide/s2cell\_hierarchy.html](https://s2geometry.io/devguide/s2cell_hierarchy.html)  
14. S2 Geometry | S2Geometry, accessed on February 26, 2026, [https://s2geometry.io/](https://s2geometry.io/)  
15. Grid systems for spatial analysis | BigQuery \- Google Cloud Documentation, accessed on February 26, 2026, [https://docs.cloud.google.com/bigquery/docs/grid-systems-spatial-analysis](https://docs.cloud.google.com/bigquery/docs/grid-systems-spatial-analysis)  
16. uber/h3: Hexagonal hierarchical geospatial indexing system \- GitHub, accessed on February 26, 2026, [https://github.com/uber/h3](https://github.com/uber/h3)  
17. Introduction | H3, accessed on February 26, 2026, [https://h3geo.org/docs/](https://h3geo.org/docs/)  
18. H3 Spatial Grid Support in Apache Sedona | by Mo Sarwat \- Medium, accessed on February 26, 2026, [https://medium.com/@msarwat/h3-spatial-grid-support-in-apache-sedona-4fcb924127e5](https://medium.com/@msarwat/h3-spatial-grid-support-in-apache-sedona-4fcb924127e5)  
19. What does Overture Maps mean for OpenStreetMap and the future of open source mapping? \- Jawg Maps, accessed on February 26, 2026, [https://blog.jawg.io/what-does-overture-maps-mean-for-openstreetmap-and-the-future-of-open-source-mapping/](https://blog.jawg.io/what-does-overture-maps-mean-for-openstreetmap-and-the-future-of-open-source-mapping/)  
20. OpenStreetMap vs. Overture Maps: Which Is Right for Your Project? \- GISCARTA, accessed on February 26, 2026, [https://giscarta.com/blog/openstreetmap-vs-overture-maps-which-is-right-for-your-project](https://giscarta.com/blog/openstreetmap-vs-overture-maps-which-is-right-for-your-project)  
21. The Case for Collaboration Between OpenStreetMap and Overture Maps | Geo Week News, accessed on February 26, 2026, [https://www.geoweeknews.com/news/overture-maps-foundation-openstreemap-open-data-mapping](https://www.geoweeknews.com/news/overture-maps-foundation-openstreemap-open-data-mapping)  
22. GERS \- Global Entity Reference System \- Overture Maps Foundation, accessed on February 26, 2026, [https://overturemaps.org/gers/](https://overturemaps.org/gers/)  
23. Understanding Overture's Global Entity Reference System \- Overture ..., accessed on February 26, 2026, [https://overturemaps.org/blog/2025/understanding-overtures-global-entity-reference-system/](https://overturemaps.org/blog/2025/understanding-overtures-global-entity-reference-system/)  
24. What is GERS? \- Overture Maps Documentation, accessed on February 26, 2026, [https://docs.overturemaps.org/gers/](https://docs.overturemaps.org/gers/)  
25. Pain-free data integration with Overture Maps, GERS & Databricks \- CARTO, accessed on February 26, 2026, [https://carto.com/blog/pain-free-data-integration-with-overture-maps-gers-databricks](https://carto.com/blog/pain-free-data-integration-with-overture-maps-gers-databricks)  
26. Enhance your data with GERS IDs \- Fused.io, accessed on February 26, 2026, [https://docs.fused.io/blog/enhance-your-data-with-gers-ids](https://docs.fused.io/blog/enhance-your-data-with-gers-ids)  
27. Enhance your data with GERS IDs | Fused, accessed on February 26, 2026, [https://docs.fused.io/blog/enhance-your-data-with-gers-ids/](https://docs.fused.io/blog/enhance-your-data-with-gers-ids/)  
28. Blog \- Overture Maps Foundation, accessed on February 26, 2026, [https://overturemaps.org/blog/](https://overturemaps.org/blog/)  
29. Overture January 2024 Release Notes, accessed on February 26, 2026, [https://overturemaps.org/overture-january-2024-release-notes/](https://overturemaps.org/overture-january-2024-release-notes/)  
30. Bridge Files | Overture Maps Documentation, accessed on February 26, 2026, [https://docs.overturemaps.org/gers/bridge-files/](https://docs.overturemaps.org/gers/bridge-files/)  
31. Databricks Spatial Joins Now 17x Faster Out-of-the-Box, accessed on February 26, 2026, [https://www.databricks.com/blog/databricks-spatial-joins-now-17x-faster-out-box](https://www.databricks.com/blog/databricks-spatial-joins-now-17x-faster-out-box)  
32. Best practices for Spatial Clustering in BigQuery | Google Cloud Blog, accessed on February 26, 2026, [https://cloud.google.com/blog/products/data-analytics/best-practices-for-spatial-clustering-in-bigquery](https://cloud.google.com/blog/products/data-analytics/best-practices-for-spatial-clustering-in-bigquery)  
33. New spatial functions in BigQuery, starting with Uber H3-js \- CARTO, accessed on February 26, 2026, [https://carto.com/blog/spatial-functions-bigquery-uber](https://carto.com/blog/spatial-functions-bigquery-uber)  
34. Overture Maps with DuckDB and other tools \- OSGeo: UK, accessed on February 26, 2026, [https://uk.osgeo.org/foss4guk2024/decks/14-matt-travis-overture.pdf](https://uk.osgeo.org/foss4guk2024/decks/14-matt-travis-overture.pdf)  
35. DuckDB | Overture Maps Documentation, accessed on February 26, 2026, [https://docs.overturemaps.org/getting-data/duckdb/](https://docs.overturemaps.org/getting-data/duckdb/)  
36. Conflating Overture Places Using DuckDB, Ollama, Embeddings ..., accessed on February 26, 2026, [https://www.dbreunig.com/2024/09/27/conflating-overture-points-of-interests-with-duckdb-ollama-and-more.html](https://www.dbreunig.com/2024/09/27/conflating-overture-points-of-interests-with-duckdb-ollama-and-more.html)  
37. How I Got Started Making Maps with Python and SQL | by Stephen Kent Data | Medium, accessed on February 26, 2026, [https://medium.com/@stephen.kent.data/how-i-got-started-making-maps-with-python-and-sql-5aaefcfb2b27](https://medium.com/@stephen.kent.data/how-i-got-started-making-maps-with-python-and-sql-5aaefcfb2b27)  
38. Fast Point-in-Polygon Analysis with GeoPandas and Uber's H3 Spatial Index, accessed on February 26, 2026, [https://spatialthoughts.com/2020/07/01/point-in-polygon-h3-geopandas/](https://spatialthoughts.com/2020/07/01/point-in-polygon-h3-geopandas/)  
39. Should You Use H3 for Geospatial Analytics? A Deep Dive with ..., accessed on February 26, 2026, [https://sedona.apache.org/latest/blog/2025/09/05/should-you-use-h3-for-geospatial-analytics-a-deep-dive-with-apache-spark-and-sedona/](https://sedona.apache.org/latest/blog/2025/09/05/should-you-use-h3-for-geospatial-analytics-a-deep-dive-with-apache-spark-and-sedona/)  
40. Working with Apache Sedona \- Delta Lake, accessed on February 26, 2026, [https://delta.io/blog/apache-sedona/](https://delta.io/blog/apache-sedona/)  
41. Apache Sedona vs. Big Data: Solving the Geospatial Scale Problem \- Matt Forrest, accessed on February 26, 2026, [https://forrest.nyc/apache-sedona-geospatial-big-data/](https://forrest.nyc/apache-sedona-geospatial-big-data/)  
42. Data conflation \- osm-fieldwork \- GitHub Pages, accessed on February 26, 2026, [https://hotosm.github.io/osm-fieldwork/about/conflation/](https://hotosm.github.io/osm-fieldwork/about/conflation/)  
43. Overview | Overture Maps Documentation, accessed on February 26, 2026, [https://docs.overturemaps.org/guides/addresses/](https://docs.overturemaps.org/guides/addresses/)  
44. Overview | Overture Maps Documentation, accessed on February 26, 2026, [https://docs.overturemaps.org/schema/](https://docs.overturemaps.org/schema/)  
45. How Esri Enhanced GIS Capabilities with GERS \- Overture Maps Foundation, accessed on February 26, 2026, [https://overturemaps.org/case-study/2024/how-esri-enhanced-gis-capabilities-with-gers-2/](https://overturemaps.org/case-study/2024/how-esri-enhanced-gis-capabilities-with-gers-2/)  
46. Enriching Overture Data with GERS \- Esri, accessed on February 26, 2026, [https://www.esri.com/arcgis-blog/products/arcgis-online/mapping/enriching-overture-data-with-gers](https://www.esri.com/arcgis-blog/products/arcgis-online/mapping/enriching-overture-data-with-gers)  
47. Spatial Query Benchmarking on Databricks with SpatialBench \- Apache Sedona, accessed on February 26, 2026, [https://sedona.apache.org/latest/blog/2026/01/08/spatial-query-benchmarking-databricks/](https://sedona.apache.org/latest/blog/2026/01/08/spatial-query-benchmarking-databricks/)  
48. Blog \- Apache Sedona, accessed on February 26, 2026, [https://sedona.apache.org/latest/blog/](https://sedona.apache.org/latest/blog/)

[image1]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAF0AAAAYCAYAAACY5PEcAAADbUlEQVR4Xu2YS8hNURTHl/dbMfAojxQxkkfyKkKhpCRD+TLwiJh4zsyEUgaKkZFMPFIkGWHCRElKKCTJ+/0mrP+393bX/d+9z9n3fh8G9/5qdc7+r7XX3WffffbjiLRo0aIxFrLQhExioR5Gqx1WO6g2kHwx1qvtZLFJ+cVCGQfEVVrly6PUnql9+RNRy0i1xywaXojLGYy5J9X+29Xuf8IitbdS3E7LCanEfVY7bnxj1F6bcpKu4hJcYofnh9pPFj2o15tF4rTaNXGxM8kHuqvdYvE/gPa999cUfdV2iYvZQr7AR7WlLDJIgBGXYr64mAWkz1L7SloM1O3pr9/IBzapLWHxP4D2nfXXFHje3eJiupAvME6Kc8gjKQmQyptgXyPwXfLmcry6APHIg5FteULlIqazEGECCxkMUDuqtlXS/bFZrb+4gZOKCcCPt6KGueKcF0lnBomLe0M6tD6kMROl8hrOFlfncsXdTtkDWPCHpaY6UE8uyx61sWpTxeUYXu1u56S/wo+1rgjE7GMRhJFXNievFBd33WgYGTkPeEqtmymjDte7QuUywlTFxLRc0BcB5MEzWx76K0Y6/BuML8YFSUy9sQ6IcUdcHLaGgXleK4NjMKKghdGPnI3s8bnj+XfqhXMdMeUpapP9fWh/Gdhy18QN8WKNI0IsbnVEixHmc4vN99Q66iR0PCy1qOWA0XvMlJHvgSnfN/dhdihjh0Ti8MpDxD6ziBXi4ng72eb1InBC28aiclNc3dJVvoSwwHckB8DoHW/KNuc5owPoOQMFzx1tV06DUzEzJK5bzkjtTgX0E1cXbwEvqrmEDgdhADUKziEWHG6Qb7DaWqPjdA59o9FSHJJEm8IpLAUWD/h7sEMqO5oiivzYgcCPtaFeMJVw7o50PNcLJ05eCPd7PWcqO6/2icUAktxgUXku1St6DNTFvBoDe9qiBi6W2ofNJVXPjv5cMH1w564Tl4d3dWGg5IA4fFZJEr6PXBU3x+M+5xCCOD4KYzrBUfqVN/zb+LYR4yULGaRyBfAnT2MxwjK1d+LagCsGWC/vG6a2198DPA9mBcTiij8JI7kI9A1mg05nu9oHFlvICMl/IxoCyWOLZTODhXg5i50J5ua7LDYxQ6W+b0kNg28Ma1hsUv7qtMK0sdCEzGGhRYvm4zcTx/CNHbGLFAAAAABJRU5ErkJggg==>

[image2]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAF0AAAAYCAYAAACY5PEcAAADbUlEQVR4Xu2YSchOURjHH/OsWBjKkCI7GTMVIUMZkizly4JPxMK8s1BCKQvFyko2hhRJVtiwUZISCkkyzzPh+X/nHO95/+859973vvf7LO77q3/33v/z3NO55z3jK9KkSZN8LGCjhIxnox5Gqo6qDqv6UyzEetUuNkvKHzbSOCTmpdX2eYTqherbv4xahquesunxSkyZTswDqY7frQ53CAtV7yW5nj6npJL3VXXSi41SvfWeo3QWU8AVDlh+qX6zacF7PdkkzqpuiMmdTjHQVXWHzf8A6vfRXmP0Vu0Wk7OVYo7PqqVsMigAPS7GXDE588ifofpOXgi8291ef1AMbFItZjMj81VT2MwJ6nfeXmPge/eKyelEMccYSS5DnkhKglRGgj+MwE/JNpdj6ALkoxz0bJ9n9FwPi6SYRu+nOq7aJvH22KzqK6bjxHIciGNU1DBbTPAy+cwAMXnvyIfXizxmnFSG4Uwx71ythNtI+4Aklkgxjb5PNVo1SUx9hlaH2zhtr4hjrUsCOQfYBK7npc3Jq8Tk3fQ89IwsjXVG1cV7xjv83jV6rgfMnUU0OtrCgfrhm30e2yt6OuIbvFiISxKZekMNEOKemDxsDR1zrJcG56BHwXO9H2U2sscvqtH9euL+mPc8UTXB3rv6p4Etd03eIGvWBAKE8tYEvBBuPvfxy3vuB1KYHNAWVUvAh7KC3nvCe0bdHnnPD717NzuksVMCeRjyMLHPTGKlmDzeTuJDawolcELbzqZyW8y7qas8sSygPWJGDftQVtB7x3rPfqe44PkAfpaOgu8OfptfeIxYzjQJ+z7npHanAvqIeRejgBfVeiliesE5xAeHG9RvoGqd5+N0Dn+j58U4IpH2caewGFg8EO/GAansaJJIiuOwhTjWhkYootG5nu7EyQvhQevH9uc+F1Vf2HSgkFtsKi+lekUPgXdx6AmBPW1SBbG/5o/NQ6ONjumDG7dVTN14V+c6ShaQh79Vorj/R66LmeNxP7UqIwzy+CiM6QRH6TdW+LXx30aI12zkIG+jL1d9EFMHXNHBetjYENV+ew/wPZgVkIsrfiT05CTQNpgNCmeH6hObHUzeRm9Phkn2EZELFB5aLMsMFuIVbBYJ5ub7bJaYwdLYf0mZwX8Ma9ksKe06rTAtbJSQWWw0aVI+/gKDVet/DzZHVwAAAABJRU5ErkJggg==>

[image3]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA8AAAAXCAYAAADUUxW8AAAAlklEQVR4XmNgGNbAHYivoQsSC/5DMclgKQOZmpmA+BMDmZrfADELAxmadYF4FZT9h4FEzciKQSFNtOZmIPZB4m9ggGgWQRLDCX6g8bsZIJod0cQxADbnOTNAxFvRJZDBMiA2RRcEAikGiObN6BIwIAfEv9EFkQBI8xN0wQAg/smAiMt9qNJg/neoHAifAWJjFBWjYLADACCOJ7pducxLAAAAAElFTkSuQmCC>

[image4]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAXCAYAAAAC9s/ZAAAAxElEQVR4XmNgGAXooAWIPwLxfyj+DsTvgfgDEP+Fij2Dq8YDYAagAykGiPgXdAl0AFK0CV0QCnAZDgd+DBAFBugSQCDIgPAaTnCWAbcNMNuZ0CWQAUyRMhRrAHE/VGwlkjqcAKRwHxC7ALEzlI6Dim9FUocVwPxviC4BBOwMELm76BLI4B8Dbv+DAMEYAEl+RheEghQGiPxRdAkYgCWScnQJIDBigMj9RpcAAVBAnWRAOO8GEB8E4r1QGiY+AaZhFAw7AABnZTlUC60sAgAAAABJRU5ErkJggg==>

[image5]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAsAAAAXCAYAAADduLXGAAAAoElEQVR4XmNgGJSAEYhV0QWxgadA/B+KiQJXGEhQDFJ4DV0QFwApjkAXxAaiGDCd0ATE/mhiYHCTAaGYC4jvAzEfEH+Dq0ACIIW3gVgQiDdCxX5CxTEASHAnEM9El0AHMxgQJsyGslUQ0qgAPTJA7INQdj6SOBiAJKeh8VuQ2HDACRUQRRL7CMQbgLgHiA2RxMHAE10ACDyAmANdcBTAAACQdCSKrBERiwAAAABJRU5ErkJggg==>