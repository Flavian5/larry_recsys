# **Technical Design: Cloud-Scale Parallel Generative Retrieval (RPG)**

## **1\. Data Foundation: Overture \+ OSM Conflation**

### **1.1 Source Ingestion & Local Development**

For the POC, we maintain a hybrid workflow: local development for logic/sampling and GCP for scale.

* **Overture Maps (Places):**  
  * **Source:** Data is hosted as GeoParquet on AWS S3 or Azure Blob Storage (publicly accessible).  
  * **Sourcing Method:** Use `duckdb` with the `httpfs` extension to query the remote files.  
  * **Local Sampling:** Instead of downloading the 50M+ global entities, run a spatial filter via DuckDB to download a specific bounding box (e.g., San Francisco or NYC).  
* \-- Example local sampling via DuckDB  
* COPY (SELECT \* FROM read\_parquet('\[https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-xx-xx/theme=places/type=place/\*.parquet\](https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-xx-xx/theme=places/type=place/\*.parquet)')  WHERE bbox.minx \> \-122.5 AND bbox.maxx \< \-122.3) \-- SF Bounding Box  
* TO 'data/local\_overture\_sample.parquet';  
*   
* **OpenStreetMap (OSM):**  
  * **Source:** [Protomaps](https://protomaps.com/) or [Geofabrik](https://download.geofabrik.de/) for PBF extracts.  
  * **Sourcing Method:** Use `osmium-tool` or `pyosmium` to filter for specific tags (`amenity`, `cuisine`, `dog_friendly`).  
* **Local Storage:** Sampled data is stored in **DuckDB** or local **Parquet** files for zero-infrastructure development.

### **1.2 Local ETL with Airflow**

We use a local **Airflow** (via Docker Compose) to orchestrate the "Silver" to "Gold" transformation.

* **Job 1 (Standardization):** Clean Overture GERS IDs and normalize OSM keys.  
* **Job 2 (Spatial Conflation):** Use `geopandas` or DuckDB's spatial functions to join OSM points/polygons to Overture centroids.  
* **Job 3 (Gold Layer Formatting):** Concatenate attributes into a natural language string: `"{Name} is a {Category} in {City}. It features {OSM_Tags} and is located at {Coordinates}."`

## **2\. Semantic ID Encoding: The RPG Framework**

### **2.1 Embedding Model Research & Selection**

Before committing to `text-embeddings-004`, we evaluate the following on Vertex AI:

| Model | Type | Dim | Pros/Cons |
| ----- | ----- | ----- | ----- |
| **Vertex AI `text-embeddings-004`** | Dense | 768/3072 | Industry standard, native GCP integration, highly optimized for retrieval. |
| **Gecko (Multimodal)** | Dense | Variable | Better if we plan to include photos of venues later; higher latency. |
| **Hybrid (Sparse-Dense)** | Hybrid | Dual | **Recommendation:** For this POC, we can simulate hybrid behavior by appending critical keywords (e.g., GERS ID prefixes) directly to the prompt, but stick to `004` for the rotation matrix training to keep SVD math stable. |

### **2.2 Rotation Matrix ($R$) Training Implementation**

The $R$ matrix training requires a high-performance compute environment to handle the SVD of the embedding matrix.

* **Service:** **Vertex AI Custom Training** using a pre-built PyTorch or TensorFlow container.  
* **Infrastructure:** NVIDIA A100 (40GB) to ensure the $X^T \\hat{X}$ cross-covariance matrix fits in GPU memory.  
* **Data Inputs (GCS Bucket):**  
  1. **Format:** All embeddings should be saved as a single `.npy` or `.safetensors` file in a dedicated GCS bucket (`gs://breadkrumb-training/embeddings/`).  
  2. **Project Structure:** Training jobs, datasets, and the final model registry must reside in the same GCP project to avoid IAM complexities.  
* **Training Workflow:**  
  1. Load $X$ (Embeddings) from GCS.  
  2. Perform K-Means (64 sub-spaces) to get initial centroids $\\hat{X}$.  
  3. Compute $M \= X^T \\hat{X}$.  
  4. Run SVD ($U, S, V \= torch.svd(M)$).  
  5. Update $R \= V U^T$.  
  6. Repeat until convergence (see Appendix A).

### **2.2 Similarity-Based Decoding Graph**

Since tokens are unordered, we replace the Trie with a Graph.

* **Local Dev:** Use `networkx` to build a small-scale Jaccard similarity graph of the sampled IDs.  
* **Cloud Scale:** Use **Vertex AI Vector Search (ScaNN)**. We treat the 64 tokens as a sparse feature vector and use ScaNN to find the top 100 "neighboring" items in \< 50ms.

## **3\. Synthetic Data: Teacher-Student Distillation**

### **3.1 Teacher (Gemini 3 Pro)**

* **Prompting:** Use "Few-Shot Chain of Thought" to generate diverse personas.  
* **Storage:** Export synthetic query-ID pairs to **BigQuery** for easy filtering and versioning.

### **3.2 Student (Gemini 3 Flash)**

* **Fine-Tuning:** Use the **Vertex AI Model Garden** or Custom Training.  
* **Input Format:** JSONL files containing `{"prompt": "User Query", "target_tokens": [token_1, token_2, ... token_64]}`.

## **4\. Evaluation and Alignment**

### **4.1 Quantitative Metrics**

* **Recall@K:** Tracked via **Vertex AI Experiments**.  
* **Inference Speed:** Benchmarked using `wrk` against a Vertex AI Online Prediction endpoint.

### **4.2 DPO Fine-Tuning**

* **Hard Negatives:** Generated by swapping 1-2 tokens in a valid GERS ID to see if the model can still identify the "wrong" coordinate.

## **Appendix A: The Orthogonal Procrustes Problem in OPQ**

In the context of **Optimized Product Quantization (OPQ)**, the **Orthogonal Procrustes** problem is the mathematical engine used to find the optimal rotation matrix ![][image1].

### **The Goal**

Standard Product Quantization (PQ) splits a vector into sub-spaces along fixed axes. However, data is rarely distributed perfectly along those axes. OPQ aims to find a rotation ![][image1] that aligns the data distribution with the quantization sub-spaces to minimize the total **quantization distortion** ![][image2]:

![][image3]Subject to the constraint that ![][image1] must be an **orthogonal matrix** (![][image4]), meaning it preserves the distance and shape of the data while rotating it into a more "quantizable" orientation.

### **The SVD-Based Solution**

To solve for ![][image1] while keeping the codebooks (centroids) fixed, we solve a least-squares problem between the original data ![][image5] and the currently assigned centroids ![][image6]:

1. **Form the Cross-Covariance Matrix:** Compute ![][image7].  
2. **Decompose via SVD:** Perform Singular Value Decomposition on ![][image8] such that ![][image9].  
3. **Construct the Rotation:** The optimal orthogonal rotation matrix that minimizes the distance (distortion) is ![][image10].

### **Termination and Convergence**

The iterative optimization (alternating between updating the codebooks and updating ![][image1]) continues until one of the following criteria is met:

* **Distortion Tolerance (![][image11]):** We track the mean quantization error ![][image2] across iterations. When the relative reduction in distortion ![][image12] falls below a set threshold (typically ![][image13] or ![][image14]), the model is considered converged.  
* **Maximum Iterations:** In a high-throughput GCP environment, a hard cap of 50–100 iterations is typically enforced to prevent diminishing returns on compute cost.  
* **Frobenius Norm Stability:** Alternatively, we monitor the change in the rotation matrix itself: ![][image15]. If the matrix ![][image1] is no longer rotating the space significantly between steps, the alignment is stable.

### **Why It Matters for RPG**

* **Balanced Variance:** The "information" in a 3072-dim embedding is spread evenly across the 64 tokens.  
* **Reduced Quantization Noise:** Each token accurately represents its specific sub-space, reducing the chance that the LLM predicts an ID that doesn't exist.  
* **Parallel Independence:** Because ![][image1] decorrelates the sub-spaces, the LLM can predict all 64 tokens in parallel with minimal cross-token error.

[image1]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA8AAAAYCAYAAAAlBadpAAAA7klEQVR4Xu2SIQ7CQBBFi0Jhce2SbJMaBGdosHjuwgUQSBSKg2BBoDDQ4FC4BggkDQECf8t0084OngRe8tPt/zO703Q978dRSo2gE/QkZVAK3QqvBXhfhaJQ8GfG931f88xCzXPBjylb8ywHU/VNQRAEXZ7Bn1A25VkOwo00soFOFbMcqQDTdODdoV3Zd6DmFA1LPFfQ1XhhGNZ5bYXie6G47OM94dM4oGArFWHTofG11k2eWehUpxnehfwazyzUvPjgO5taEA5MAf5hT8gqzXaNxRg6Qwf1vsdH6GE7Aa5jmzbYQ1kURY1y/ufreQGX/1lQSDJfUwAAAABJRU5ErkJggg==>

[image2]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA8AAAAZCAYAAADuWXTMAAAA/klEQVR4XmNgGOFAQUEhUl5efgsxGF0vg7GxMauioqI4UPI/CCsrK4uJioryiIuLc6uoqIgCDTeXk5N7BJJD1wsHUM3/0MVhAKdmWVlZE5Ak0IZ+dDkYwKkZ6qf/QCcKIAmzAMXmIan5hCSHADD/ooldBoaFPLIYVgDTjI7R1WEAoOlmIIVAJ3ciiekRpRkYSOVQzR7I4kCxuzC2tLS0MJBiRJKGAKCiz+i2gOIeKOaFpOYnsjwcEPIfKKEA5fehizNoaWmxQTWfRpeDAajBLOjioHQ9ASQJTCR+WOQCoAajOhkosQoo+AeI/8GcjYZB4r+B+DtQrQGK5lEwVAAAl+pasFHL8aQAAAAASUVORK5CYII=>

[image3]: <data:image/png;base64,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>

[image4]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAEwAAAAYCAYAAABQiBvKAAACx0lEQVR4Xu2XS2vUUBTHWyqIC0E3PmfmzjgDI+jCjyC1uBB1JaLUDyC6F0F3bkRc6kJFsbjxMxRXvsBXUURFceFCrKL1LaXqtP7O5ARuztxMZgpmQPODQ5L/OTn35OQmNxkaKsgP59xj7Bd2A3uJLVSr1clKpXJH91fYc/5raMqUt38Fe+8dn4j3C4DZs5HNSHwsMwo7FB8zy67G+/8sXPBp7ItevNgsNuOix66t0aiqPU8Qn9VClEql9cR+8MZoYR91rFg7bM/LE8Yfx7559Yj9qNVq221smzgooF8TvVwu132dRM1QfDeIfx06h/uxVXS256wvb5w2zeodaMNuBfRR9T3xdXn80KZ9LQvNEyymmy9PeqqDO7tPgmjCmPWhn1ffZaPLjDjoa1loMTetzuxdp75568uZEa3jnnUkIOBpWlc1QYcvpHWDhu+Vc9husz70dzrOWuvLE2o7onXssL4EoaYwe7ag/cZe+TrHP7HvanOux1nhAjel2WwuR3uEtSh2pe8bBNTx1dYYRBs2Q5Pusn2ojVhoNBpLbexi0THkMZYxprBpOeZx3G1j03DRd1/QyDshrw32L2EXsQuszg2boxtxjVZPEL+/sFFf5/hZ5sl9ILkY67qvcYEHRGfFXe3rA2KJ9uG+dSQg4HmoMVzcSdHr9foq6+sXcu2XXHZRYQYs0yLP+vogiN9fbHdaXwItuKNhLnpHiT5sff3i9J/T6jTyqI5/3PpCEHeqH+PiN9kcaTj9eLd6BxKE3U7RsxP0QFoup1/6MgOtL2/SakxAwDEJ4k7sCvgSCTKTpSCrn+Z6YH0u+j2S8ffo8VsbkwfcsDV6415YXxucZ1z0C/DJRf+Nn7GWH8PqtVkv9A02K58Avj8LmrDBRY+15I7HkE8S/1trWMeYd9HKOe75/jq66EiNcR9kO5f5HisoKCgoKCgoWBx/AC9gG3EnWGLhAAAAAElFTkSuQmCC>

[image5]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABIAAAAXCAYAAAAGAx/kAAAAzElEQVR4XmNgGAUkAXl5+Wgg/gnE/5HwGyT5X2hyt5H1YwBFRUU3qMKnyOLi4uLcQLF/UlJSXMjieAHMVnQxZD5RQEFBYSXUsGYQH2oIM5oyogAjzFVA/A1osAC6AqIB0IDfIIOAhtijy5EE5OTkTkJddA9djmgA1DwLaFA5zHvo8kQBoMa/xsbGrCA20DBnqGF30NXhBUANn1VUVETRxEhzFVDxE1lZWRN0caSkUIMuBwdAyRZ51KT/Ek1+LZIcCJ8DGlyIrGYUjHgAAJu5SCdhDSIOAAAAAElFTkSuQmCC>

[image6]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABIAAAAXCAYAAAAGAx/kAAABBElEQVR4Xu2SPQrCQBCFLUQQQaxMkX+TKlZpBQ9h5RnsvIGHsPckdmIviJ1HEAQjikKib2BXhhF1Uwb8YEgy783LZrO1mgG+7+eos+yXAgEPz/P6rusOcX+TuhEYTC3LaunnMAwthPa4pyLgU0LaVNoXVlemn4R25PNvBEHQ0WapUc9xHFv2P6KDMNRkvSJJkgb3/QSrmqiwFT3jmkVR1JU+I/SqUHtUKnVjMLyjIJydmdRKgZC5WlEhNWMwfKeNxnWjwkbS8xMMHWzbdug+juO2Csql7ytqXwaiR0F0puq8/xGY1/jtU9lnR2EptRcwjWHI9FtRF67TH2Ma1Ra14J4/VeUJtChRaXW1tPQAAAAASUVORK5CYII=>

[image7]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAF0AAAAZCAYAAABTuCK5AAACw0lEQVR4Xu2YPWhUQRDHYyf4ARbHIee7vYODExUbbSysBU1rIUjSiKIgFgHBQuyDnYWFokQLjQlWFjaCIEjABBUEC7EIMYIYUTR+4Ff8jzcT5g2by713ex+Q/cGft7MzuzO7773NuwwMRIKRJMle59xSuVw+a32RDoDN3gd94/ZTaNTGRJhqtVqkpxOahh5y+xn0APpJbTvGB+ImtY2n/bK2IwraaGMvaRubd0HbkQBgU29LGxt+zLPpO7UdHLqrSLrIrxhpxsZo4J9VsTRuxMZ0C+R+rmr5r0qlMq78KR/WelqP55h5PaYZIfKl0MHWJ+As3A3/eZ5wj/X3CtTzmDfgpO6HfQL9L3Sfhcdtt/3NaCdfCgS/cfzEW58A35wktL5egsWup5p0XXgodtCadJyPPGtpJ98yCD7oGmfbvZWKQP9dvqaS9QtSV61W24wN2OL4U64ZiDmVdy158qXAgCm+jviKKBQKG+V84mQTNqbXoL4hru2Jbw0+EPe+1fPckidfChlE5zS1S6XSNuP/QVcUeID89Cppf7/Am7DqBiDmD/QV+gJ9h/7amFZoNZ8XDJxXbdrUIbHRPlMsFjewbyprEsTfXEE3cBPHMP91tK9BV6ErdnwWqDYS5h22vk6QOx89vVj4cbF5kjFlLx8lkkTsfgJ1fYSq3aqxrXyOz3Nl0yRz3H5rfbghd3RfP4C6XsonrGwCfWHYuFC0nc/eJZkkSZL90C7Vf4j6yxl/rWHMaBbZ8auBeu5jwUeUfZTqhB7puFAEyYfgd8Ze4Elemf4Z6td9vQb1XPLdKK4/eK0h8q1D4GtoVnfCvuWbIMvEnQZ1bHWNH2reesTnAv2LIkg+OC9Cn6APrvHZ9Ft8eF0GocMqlj6tJPYz9Auv1znxdxvK7xp/xKSeBePX9VLcYr1e36RjstDtfJFIJBKJRCKRNcs/g31LmzF/i0kAAAAASUVORK5CYII=>

[image8]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABUAAAAYCAYAAAAVibZIAAABNklEQVR4Xu2QvUpDQRCFY63t5RY3969VSWNtLaitrZXYiUUqi+AL+BJWgoKVzyBp8gRiEaKCoCD+gKAQv4HdZZi7iVW6e+CwO3POzOxsp9NioSiK4rQsy084dRxZjwb6WHmlrm89Aco4tZpHXdc99IF4eMyG1RvA+OAmz2yKNoG38zwBmLbhAbyZVUD+2p1ztwnANHRnP1aQJMkK6x45jzS9sp4GfCP5J7lnWdY1+recVVVtiY5vVetRYHxUdyna9zH34zRNl502jG3SgEyn8NDHUkTuXMVhVbf6/01luomlcOLuT1Zj4KXORWEn+9fkeb4J11V+R/Jstab9UWB8NvGLa3xn8iP7gBiWMN3DsU4SX8SK/QY2H4B4Bt/gK/yAv15jvV24p7xfyvsOf/jXE6+3aLEg/AEIbWzZqWo6NQAAAABJRU5ErkJggg==>

[image9]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAGUAAAAYCAYAAADjwDPQAAADs0lEQVR4Xu2YSWhUQRCGk4Ai6tEQSWbSExXUUfQw4AaiB0Ex3oLoSVGCCygKEURQPBg8iOJyEERRXEAwoBc9eHILkktE8BREJcQVFdQYiMZE/8pUxbLm9RtNJi8H+4Pidf9V3V2v5vVbpqwsEPhfcc41wH7C2mCt3H4Au0vtTCZzwo4JjDIofI+00+n0dPohpJ/NZsfjR1km/YABxapHgU7W1dUtEK22tvawjhkOmHObtLHGFdg75Zsl7QKw+EEEf+WtRdZuYzTwd6pYGtdkY0YbrPlB5SB57Ff+DuPv0uMF6EvIjwJtoisX7c2wftgT1GUvx7w3c/XCFut5EHtPx6B/XPsJ0rHOdqvHoie1PgFX0jz4D/DCOetPEuRwjPNYYX1MOfzdVhRQoKl8ruVGX056Lpcbp3WuzQ+taeBrxdiVVhfi6uoFg1463jHWJ8DXBXsYF5MUyKEvLg8UaCtst9UFjB2A3bI6ETUv/ygFOlFZWTmZamN1gW5VvrFeMGA1rBF20zcY+nU+epNLkmJ5cJH+2AUaHt9idQL65wjNu56L2UEEfpRriHlj9VgwoI2PTVEL05WA28ROjvGeTFJUV1dPpDyQ023rE6LOQyNFdub54EPirY4ctqDo+6yuoXHDep7QkZ4T1K6pqUkZfy8d6Z7Jxchqf9Igh2Y+0UXWJzj1phMF/D1SaLZ+zLfOxgmOH/gR+oDVBJqT1+l2+RcEb2wBCH6l2lT0DdJHe1dVVdUk9tFHUEFicSD+sscuoQgXMf8FtM/DzsHO2vFRIO5bXB6Yd+PfXJmYo4XmMbbDxhHQb7B/ttI6sGun6LiSQFc/bUHp08JULNUfulVJ4tIfK4rlAd8LqxUDY07FzQt9D/sbqZ9KpWagTvdtXElw/DxRfVp48E0Cx9fWRw8trSUN1p/AOd6xPsFXWAKv9TPpmWR1gueNHEu3Sj7/wQvWF1cS7OSSWDqdXgqbq/R60rGr5uj4YmDMkX8xO96CoiykPHBcb30Eij4f/tNWF+B7ZDXB5e/9n6zOVNC6sOdY+wxslQ0oGc48EN3vr+WnRm8nXWtjBeWhn3uaYjmSnz6CrU7wvNOsLnBdyHw/3IihL95nsE4ton816sQkIauPBcjjreM3QoE/4L7bL3GL7zyo0K7Iq74aW2F9IwYTH+UkPrr8lh36+MGVsga2VsXSK53EfoH1ZYq8lyeBy/8FTkWi/On42MZYXP4j+RDtBh5DRudGxwYbb+G4ZqsHAoFAIBAIBAKBEfMLm3lplwXOV64AAAAASUVORK5CYII=>

[image10]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAFIAAAAYCAYAAABp76qRAAACyElEQVR4Xu2XO4sUQRSFRwxEQcwUhpnpngcMiKCYGJksCoKYmymYKKs/QDQQQRQxNFLxiYKCmAoKa6AGRrK+Ax+Lb2Hw7erqqOdu35LyWFW9PWpvUh9cuuuculW3qmu7ZyuVSKQMkiT5gbiBGNH7McQFxDtpc/+IgzRNd+Ay07R142ZY7RFzHwmAjXpq7mu1WodPINrDdvsXMPYh3uoRlhhH9BBfjYanlHLe/wbznkL0rbq+I85Rn9WWL/HK8p6TJ+u5b3wsaSv5n41ngHYS8YL1IGZAh35R9Hq93mavDHx1GRqNxkqpkXWh1WrN0/zL7BlCY4uHDd/IehCd8IpDH1LvFntlkLeRIQ+bvFt8HILl7Bng91gzhMZ2gl1fK0mYeAV70A+od5S9MsDcj30LQt1nEMtYNyBvwpcrwNuA2My6gPUuDOU6QcJtX5LoPq8MkuxdKTUstXV8CGYnOe+vvNrhjVWsL7INvLOIl6wHcU2IJ70E2jfEI1svG8w/LLXhhGwh/YvdZqrV6hzNO8+egdcsYN2j0D8i3iM+IfpT/j7oRvYwyDVcr0uRonU6nVnc1wf6n/DEcYx7DAs6gvvDiEOIg5zvo9lsLpZaMMZpo+F+fZr93vOCnJ2aF/rTD57oQqT6fkQM2Trad0S3telC63tit23fRaKHgXUD1r0OsYn1gcFk91wTYpI9orfb7fnslY1u5GSNuN7EKV3AfRg7xwW8h6z9Fb4JoX1Q3fkyZtB3b5Hg/BBShwReDy1cr7LvQnPusm5Ict6xhdEJ/yjOFM/6dGBqKVKP9n/AuiAbjA9IlfWBwYDbZEI86TUO77fCiyziX5Nkvx7kw7GKPR/ov8tVM8YYlf+GWB8ITLA/yT7vr5Ps/+o3iL7dB09skW7mM8R4t9uda/tlgvkvSZ2s54Gc7bqGyQeBmJDfn9wvEolEIpFIJDIlfgKB0BJ8V5LKPQAAAABJRU5ErkJggg==>

[image11]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAsAAAAXCAYAAADduLXGAAAAd0lEQVR4XmNgGAV0BTIyMpxycnLOCgoKHvLy8l4gDGIDxVxQFAIldgPxf1wYqMEVrBDIyAUKLIZpBJq2EYgl4CYhA6Bia2Q+yCRkPk4ANNGBaMVAhfeA+AC6OFYA9UwsujgGkJWVlYI6gRldDisAujkcXWwU4AIA3wEdoSsTr7QAAAAASUVORK5CYII=>

[image12]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAJAAAAAYCAYAAAAVpXQNAAAFGklEQVR4Xu2aa4hVVRTHTUWyh1gwjAwzd5954MQFEcOyog9FFBYWqSiChIYIfjdDEAQ/VBpmkUifetCn6kvQk1JQP0j5lhIU1CAKBRHR8DE6M9p/ddaWNf85j32P9053LucHi3P2f629z9qPc+553AkTSkpKSpqK7u7uF1grKU7R8axUKlXWmp4oitbA1rNeUpxqtTrFOXeZ9TxQ5zZrTU1vb28Xkj7LujJROhRoW7nyeCWhb6nGdS3wL4QdZj0Nufogfi3r0J7i46YZ2ljA9RuKHBRXn3tZtyDmB4nr6+trY5+gbbzK+ngGP0GP66RsZ58AfR/sGOuMtIG22llPArF/s2aB/x9pj3WPzsN01huGi1f2DdYZHcjUxLHq93Z2dj7M+ngG/f1O+pzWL/iehX3IOoOYVbDrrCeBuGusWfLmIWQu6woOOBhy76OJD5O2w+zvMa6WIGmycKI8j81E2ce4PQP/YutPg9tJAm2/hriXWDdM0pyOWBHlj83+iDlqOJIQzrCprFu6urrmauLvWd0OCjr/nPW1AtpnPmkG/L7+nE8y7lS0razFkTv58L8h7WCsXyb9D7MftKDrQn9//4N2EaSBmO994tg+ie1SbC9h+ynHtgo4aR7TSf8aV5p52L4I2xkyXkmg3k2M10+sWxBzgjUL/Jd1Hp4WQ14rUR7CdgXHjgku/g3PHRAdSEn8bWy3wb6Vck9PT4VjWwWnDw3o8zvYvovtZzoOn3BsCKh3EvYn6x74tuJG27FusfOARfMB9ndLmePGDCTxekgCmvgga7YsZ4QtFwXtzkFeB1hPAz+/s+QnNsTa2toe4Ppp+MmyGvr4kbzyMDELrb+9vf1+W7Yg9kduz5LlUyZrTr9akevhpJ5py0UJmge59HECDGKekBhsN1vdmcdNDOwSlBcZ36GowCM92tmIujtyEzfI214sjldCLO0VRBI6WSNOGuT1jSneg/JvvoDcF7iMR3qntwGsC7Kw0daXrFvgX6/zMN/q0HabfRm7GaY8lPYEmUXwPPjFwboFMT9LjNwvsc/DbXC5FnC8TbmJNxinL+2Qxxb2eeD/xV7RUD7q6Ipkge8E7CLrgowxNpNZt6DulbxxZT+XayFoHrDSHso7iPizYuDbjna+kH0c8CsfL4byIxyfR1DiDQa575L8ccWaxj4BV7NePyaV+M3xnT77sWDgG9CFMoqs8fX49ln34Lj70f4a2UfceZsTx4YQPA9yAPlmw7qg33IkiVvsg9YNO8MJyk8FtKNWqwVJHPUPsj6WpA18R0fHfdA/F19EP9FJ8ZakOoLcw8G3gXUL6s3QnEa8/xHwEz4b+iU+PsrbsKjet1otBM+DJpb07eUmbFj9bLdgQ7AbsNNU7wgPFLTraRaNvK/wiQd/O6onOO41F/dZ+nebTDTxybhcsfX0pEm9/xGkDdYEl/HpAr5HYYN6bJuLNz8PA3zlgzZM9z/yTXPU+BtbZ2LD5wEHftPRgNwN0jHWakETH3WmNTOSrzMPEQz6tAz+c6wLMnms1YMxnQc9WOZNXCg+8UrBl4yaeObZ3GyYPleTHjbEH5mnI08lfiGb+Xa6IP898ssOtsfZGUJN84Dg+Qg+xXoR0M5fsENpN6BZoN5V2EXYBRf/j6Yui7rRINe3XPxSb9RnBBe/rN3DuuAa+OHTxU9tv7MeQqF5cPHb1tWsl9wVct9xlUVPZN4jtQTR//VNpUXB4lnOmiXK+Q9WSUlJSUkS/wIMCe7ADVOBEgAAAABJRU5ErkJggg==>

[image13]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAEoAAAAYCAYAAABdlmuNAAAB40lEQVR4Xu2WO0sDQRSFI8RC7I1gktmwlhZCyhQWKoilhfgDbCz8C7aCjQhq4wsVrCy09IGFlZZ2dhYqNuL7AUGM54YJjNe5iRuXjOJ8cNjdM/fuzJ59JImEx1MvSql37nkYCGkeKnHfY5BKpVqDIBj4q0ElsfAiNyuk0+nObDZ7TBcH7fPxKKD/Qm//TlBY7Km++LL4OIG732OO4bhbqq0Fwp6s7Nd7Dqdg0Q/SwslHOGPMK0JHxvEZ9CqJavL5fDOCGjZ6rPP9aqSgwjBsI5+2po/gdm311UD9KHRgqERbXlcG73oLUu3VH7NBEu3D6+O1jUQKCuuaEPwVmx8FsR8DezpFqzB5P+9pFEoOasvmw5uz+d8BD8YIem+ha+j+0yAmHIe5XjlG8TbUbtbUgvoFreFcq/ouL0OL0ALvr4YSgsJ5DwV/hny8IR187EfgIgrmsW1yl0hBwdsQ/FntJ/lYnNB/li+Tu0QKSvpGwVuy+bGCyacxyR33a4GeqSji/dVQclAF8uP41YsMTUBhcd8lUlAE+dAQ856gG9OLHZo4k8l0cd8lWNOLFBRu6g7G3gyriWrxVAWGFy+5XE5JC3IB1vIIXUHnWpf0pOBGhqzuBHqGNmn9rv/3eTwej8fj+Td8ADxDv3EMGCPAAAAAAElFTkSuQmCC>

[image14]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACYAAAAXCAYAAABnGz2mAAABZ0lEQVR4Xu2Uu0rEUBCGVxcLG7GwCiEnRZ7AvIsvIIIXFIW1tLNTLK3ETpbF2sraxkrFywNoI1Ze0GUb/cedwGTOhS0OLkg++MnOP3PO+TcJabUa6hhjXqED1ofujw2EGUDf0InujRUEOtReECzo5Hm+ov0K9HegN+gTWtT9UaFg0DF0hrKt+7+g2RW3lrSqZwj499C5qG+hCzkzKvjzN9VvOlP2nPiCFUUx49qAPBwyy+Uk6q+AtmuLGdojy7Il7dfwBYN35QsGHWk/BOZ3ob6oaY+OnLEIBCPfF8zyQ9A7DOVVTevLspwSIzZ/EYzAmjtk65nhI57XfQs+aM3jWwF8fnToELyI6y7fFcDnR4eDbbh8VwCfHx06BM9+0+G/uwJwsAftR4eDbWkfd3HBFwy9UvtRSZJkju/Avu4RHHpZ1HuusNHA5qfQC/QEPfL1GRrIuTRNpzncJa7XZviRnJAzDQ0N/5Ufue6MUlyxQmwAAAAASUVORK5CYII=>

[image15]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAJUAAAAYCAYAAADzjL9JAAAE10lEQVR4Xu2ay4scVRTGyxjfmo2MA/Po290zOmHIQpkIblwYFQXxtVEXuhH9A3xANPENgkgQRAVxoyA+cBdXLlTwGTBjjG/FhQlBUUhGBzOZJDNJ/A51brz19ame6rKqu4bUDw5d9Z1bdc8991TNraqJopqamppVQ7vdvoS1Vqs1xVpNMczMzJzBmrKWhVWLc+6EoS2yVlMMjUZjM37WsI6cP8XaqiWlqA6z5oHvT9gxOU7tb9g/wf4HfMwgQTzbYPNBfIuwA7AlrzUBH1cW6Oqh6BQtqiOshcB/j07GdeRaqxO1QPrA8QVk6O+LPj4+PsG+MkDOHo5O0aI6yloI/L9YxwlpkzdoNK7PDH2T+r5nXxmgny1RBYsK/d/AWm6sAoC2xFpIt8Lp5hsUuDvcITFhPXMN+6C/or7X2FcG6GtrVKGi0vEvIUfr2ZcbqwCgLbMWooXziaHfLz4E+AT7Bgli+sEap6BjMX1lgL4eiSpQVC7+sz8/MTFxEfuyMj09fSbm+vqOZZCVUGjHWPPgir5NjsHv1aEO7TmdoFtDvQpYhYNEXAptGbYn1MsG/T0aDa6oZM0rF9jesbGxc9iZFRx7seZ0B+xB1MJdiQacbNWOs+bRoORu9AV+d8J+0g467lxVQeM7oDHvhh0RbXJy8ixuWzaYgMeiPhcVxrnOxU/ts5HRdy/4pUSr1Rpm30mkQRbNoxOU8MsLPdVfCHXP8PDweawxo6OjF+IJbGMW6+XlrE8CbFOoY/9HHkcR4JyXSfGy7kFRPR4ZE+uMooL2Omw/7DiOexu/b8I+lLiRhyu5PYM7yijaLiCed9mXF83lz1H8stZbEiuxlubRk36aopvHpekhKBSHRN2U0VZMqEcSYPWPRD8jurWmgD4L/y2sr4TchXDsS92KqhmvNzMVlQD9KOxZ0u6OrMkk0Nd6tF1GXC+zLw/I++U6zztwzve8cTtzwi1NwAnuFB9+ryWXfz/V8dSIgd0M/WvW+4XG1TEeaAdVP83wdbTPCsb7ZMFFdWJoaOh82R7Xd2nI/43JVt0J7ljb2dcLOMcDmpuO+BNYCbQ0Afoeywdti06e3BZDXbSTFvr6hfb9eYqeiAlJfyeMV6700J+FIotqZGTk3DBGbM+F/l6R4sQ5ftP4Oi6mlWjGDzcrr0U5sWmaYE2EgM7uU993fh92hWxb7fsF+t4q/VtXNo/Fb8ufV2x/9V/L3pCiwvE7Wff0UlTQnnbxJ7FvYYcwjo+4TU5OlzHCfkU8Z7OzGy5+Yg5fIq/Bengs2LcnnTXsL7j4+5l8M/tLBohgnqc2f8hxsLdc/JTh9Y7zlw36fNHF3yMlVolZvk8mXpOgeDZovL/DFqempi7QY3c1aT0l/jRr0iJYi+rLUAvpsahkPbUt2L8q9BcB4tmO887JgxL70pB1lNOLUgs9edcTR0JI0fIgVz2C/ob1KvN/x65FtYt1T49FJX9q1rFeBiiOe1nLjZVES8sDEvgx7HbZbsT/8lF5/NgR76vsy4IW1W7WPVmLSt8tFTIPfccK3NLy0G63Gy5e3L/BvqqCWPfBZvPcIVy8TJhz8bul+ch47M9SVNg+pOeSP+GHXQW/UnTFKiBLqymGLEW16sG6ZyNruPXPsFZTDPKagDUhTa+pqampKZJ/AQ7musCswX99AAAAAElFTkSuQmCC>