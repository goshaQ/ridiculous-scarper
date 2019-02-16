# ridiculous-scarper

## Getting started

### Docker Compose
The provided `docker-compose.yml` is configured to launch two services, one of which is the graph database and the other is the scarper. The easiest way to deploy the project is to run it on a single machine by the following commands:

```
git clone https://github.com/goshaQ/ridiculous-scarper.git
cd ridiculous-scarper/
docker-compose up
```

## About

### The Scraper
[The website](https://www.e-krediidiinfo.ee/) we are interested in allows to search for a company by Register code. Because our goal is to extract as much information about Estonian companies as possible, the obvious choice is to iterate over all possible values and generate a search request every iteration. In the project, the `CreditinfoScarper` class takes this responsibility. 

The information about a company is retrieved from the server response. Basically, we process each row of the table describing the company manually and extract usefull information. That is done in that way because the data have poor structure. After extraction, the result is stored in the graph database. But if there already exists a record about the company.

To speed up the process we employ a data parallelism approach, that is we use a thread pool to process the result of each search request. However, because of some restriction on the number of generated requests per second imposed by the website, we actually make a delay between search requests.

### Graph Data Model
We use pretty simple data model which contains two entities and single relationship between them. For now, the main objective is to capture the relationship between a company and its representatives. All of the company details are stored as properties of the corresponding node. For sure, this is not an optimal design, however further improvements require a better understanding of how should the interaction with the database take place, or what the database search queries are of interest.

The definition of the data model is shown in the image below.

<p align="center">
  <img src="https://github.com/goshaQ/ridiculous-scarper/blob/master/assets/dm.png">
</p>
