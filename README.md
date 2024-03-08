this app:

- keeps track of swedish news by either scraping them from news websites directly or using rss feeds
- groups them by meaning using openai's embeddings api and dbscan clustering algorithm
- for each group chooses the most representative headline by finding the closest headline to the centroid of the cluster
- translates clustered headlines into english using openai's gpt-3.5 api
- serves results over http

i have build it mostly for myself to keep track of what's happening in sweden.

inspired by:

- https://newshavn.duarteocarmo.com
- https://thetruestory.news/en
