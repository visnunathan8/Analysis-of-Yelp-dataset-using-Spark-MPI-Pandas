# Analysing Yelp Dataset using Spark and Comparative Study with different distributed processes

## Dataset - ( ~9 GB ) 

https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset

## Tables used : 

Yelp_academic_dataset_business.json  

Yelp_academic_dataset_checkin.json

Yelp_academic_dataset_review.json
 
Yelp_academic_dataset_tip.json

Yelp_academic_dataset_user.json

 -------------------------------------------------------------------------------

## Architecture Diagram



<img width="619" alt="image" src="https://user-images.githubusercontent.com/30067377/209013175-0d2a6619-a9ec-49e1-a79c-6fc1e812e8f6.png">

 -------------------------------------------------------------------------------

# Spark Features Implementation

1. Persistence
2. Lazy evaluation
3. Fault tolerance
4. Data Partitioning
5. Parallelism
6. Transparency

## 1. Persistence

<img width="702" alt="image" src="https://user-images.githubusercontent.com/30067377/209013276-9b93ecb4-0b18-4752-861f-69de66836d4d.png">

## 2. Lazy evaluation
<img width="642" alt="image" src="https://user-images.githubusercontent.com/30067377/209013336-ae751e31-5554-4d8e-abd7-7bfb20b2ed17.png">

## Implementation of a Distributed System to execute Spark Using Multiple Computers (1 master and 2 workers)
![image](https://user-images.githubusercontent.com/30067377/209016386-48c7184b-f2e6-4128-b61a-f46f3df4e69d.png)
<img width="518" alt="image" src="https://user-images.githubusercontent.com/30067377/209016094-31d19499-f94c-45d3-b665-4ddc85556dd9.png">
<img width="852" alt="image" src="https://user-images.githubusercontent.com/30067377/209016297-855492e1-94eb-41b1-9cf7-21b584f7dfb4.png">
¸
<img width="720" alt="image" src="https://user-images.githubusercontent.com/30067377/209016442-26170a27-57bf-4d33-9458-792be5b42174.png">

## 3. Fault tolerance

<img width="672" alt="image" src="https://user-images.githubusercontent.com/30067377/209016592-b91b8b76-c8b1-4b5c-8d78-6e4299db2fb5.png">

## 4. Parallelism

<img width="559" alt="image" src="https://user-images.githubusercontent.com/30067377/209016671-3401f5a8-f835-4e94-8e09-cedcb7b86797.png">

<img width="759" alt="image" src="https://user-images.githubusercontent.com/30067377/209016689-c4403e03-fedc-46a5-ab30-e5b99fb24b42.png">

## 5. Data Partitioning

<img width="598" alt="image" src="https://user-images.githubusercontent.com/30067377/209016751-9fe8e036-9d9b-4156-9533-91ee84dec6a0.png">

<img width="720" alt="image" src="https://user-images.githubusercontent.com/30067377/209016771-85142942-f97b-4b86-9945-3d7e932f8750.png">

## 6. Transparency - Data Lineage

![image](https://user-images.githubusercontent.com/30067377/209016826-54c266dc-d15b-4b12-87ab-a07ce37cdb06.png)

![image](https://user-images.githubusercontent.com/30067377/209016849-41897a22-6b78-4b0e-9282-5d7169abd7a2.png)

![image](https://user-images.githubusercontent.com/30067377/209016894-ee41bfec-073b-40f5-9d61-e53cb58e1086.png)

## Ensuring transparency in Spark data processing with the explain() method
![image](https://user-images.githubusercontent.com/30067377/209016931-520bd5e7-9721-4b14-8749-a831752f4b8f.png)

## 9GB file pyspark execution for the usecase in local system(8 Core MacOs)
![image](https://user-images.githubusercontent.com/30067377/209016975-1dd6d36d-5663-4207-889a-7dbbf3d297e9.png)

## Yelp Dataset  Analysis & Comparative Analysis of Distributed Programming Techniques

<img width="383" alt="image" src="https://user-images.githubusercontent.com/30067377/209017041-2fde3252-ab03-4da1-b09f-86b23c9b9f03.png">

https://public.tableau.com/app/profile/san.vinoth/viz/YelpDatasetComparativeAnalysis/YelpAnalysis?publish=yes

## Additional Works - (Hadoop Cluster)

<img width="770" alt="image" src="https://user-images.githubusercontent.com/30067377/209017089-770df7c7-04bf-4635-9576-26404210da09.png">

## Project Components:

<img width="422" alt="image" src="https://user-images.githubusercontent.com/30067377/209017156-474dbdaf-3f96-47f6-ab7c-3fe3fded27ca.png">

## Learning
Spark

Databricks

Azure Blob
 
Tableau

Setting up standalone clusters

Hadoop environment setup

Team Work
