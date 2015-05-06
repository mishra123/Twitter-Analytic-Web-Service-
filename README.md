Twitter Analytics Web Service Project Introduction, Guidelines and Objectives:
Introduction and Guidelines:
A client has approached your company and several other companies to compete on a project to build a web service for Twitter data analytics. The client has provided a raw dump of tweets that runs into tens of gigabytes. The dataset will have to be stored within your web service. The web service should be able to handle a specific number of requests per second for several hours. The client has a limited budget for this solution. Your task is to build an effective and cost efficient solution utilizing Amazon Web Services resources. 
1) Front End should be able to receive and respond the queries. The interface of your service is REST based. Specifically, the service should handle incoming HTTP requests and provide suitable responses.
a. Users access your service using an HTTP GET request through an endpoint URL. 
b. An appropriate response should be formulated for each type of query. The responses format should be followed exactly otherwise your web service will not test properly with our load generator and testing system.
c. The web service should run smoothly for the entire test period, which lasts several hours.
d. The web service must not refuse queries. 
2) Back End system is used to store the data to be queried.
a. Millions of tweets are used as the dataset, and you definately need a capable database to manage the data and handle the queries.
3) Your web service should meet the requirements for throughput and latency for queries for a provided workload.
4) The overall service should cost under specific budget. Your client has a limited budget so you can ONLY use m1.small, m1.medium and m1.large instances to maintain your front end and back end systems. You can use spot instances for batch jobs and development but not for deployed web service for the live test period.

Input: 20,000 tweets stored in JSON format.\ which are stored in S3.
