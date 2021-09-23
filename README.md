# knn Movie Recommender

- This knn movie recommender project was done in 3 steps:
    - Milestone 1 (M1): Implemented a baseline model which considers only the relevance of the user bias, in the form of user average rating, in predictions and then averages normalized deviations from each user's average. Improved the baseline model by incorporating some weight that takes into account famous movies
    - Milestone 2 (M2): Implemented a better model using knn and based on similarities. Compared two types of similarities: Jaccard and adjusted cosine based similarities. We found that the adjusted cosine is the better strategy to use.
    - Milestone 3 (M3): Optimized the model implemented and scaled it using Spark