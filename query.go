package main

import (
	"encoding/csv"
	"github.com/blevesearch/bleve"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"io"
	"log"
	"os"
)

type Dataset struct {
	ID                string `json:"id"`
	Name              string `json:"name"`
	Rank              string `json:"rank"`
	Industry          string `json:"industry"`
	HeadquartersState string `json:"headquarters_state"`
	Revenues          string `json:"revenues"`
	Year              string `json:"year"`
}

type Query struct {
	QueryText string `json:"query"`
	QuerySize int    `json:"size"`
}

func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}

func createDataset() (datasets []Dataset) {
	isFirstRow := true
	headerMap := make(map[string]int)
	f, err := os.Open("./datasets/kaggle-fortune500-1996-2023.csv")
	if err != nil {
		log.Fatal(err)
	}
	r := csv.NewReader(f)
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}

		checkError("Some other error occurred", err)

		if isFirstRow {
			isFirstRow = false
			for i, v := range record {
				headerMap[v] = i
			}
			continue
		}
		id := uuid.New()
		if err != nil {
			log.Fatal(err)
		}
		datasets = append(datasets, Dataset{
			ID:                id.String(),
			Name:              record[headerMap["Name"]],
			Rank:              record[headerMap["Rank"]],
			Industry:          record[headerMap["Industry"]],
			HeadquartersState: record[headerMap["HeadquartersState"]],
			Revenues:          record[headerMap["Revenues"]],
			Year:              record[headerMap["year"]],
		})
	}
	return datasets

}

func indexing(dataset []Dataset) {
	mapping := bleve.NewIndexMapping()
	index, err := bleve.New("bleve.dataset", mapping)
	if err != nil {
		panic(err)
	}
	for _, el := range dataset {
		err := index.Index(el.ID, el)
		if err != nil {
			return
		}
	}
	err = index.Close()
	if err != nil {
		return
	}
}

func postQuery(c *gin.Context) {
	var queryText Query
	if err := c.BindJSON(&queryText); err != nil {
		return
	}
	index, _ := bleve.Open("bleve.dataset")
	query := bleve.NewFuzzyQuery(queryText.QueryText)
	query.SetField("name")
	query.SetField("industry")
	query.SetField("headquarters_state")
	searchRequest := bleve.NewSearchRequest(query)
	searchRequest.Size = 15
	searchRequest.Fields = []string{"*"}
	searchResult, _ := index.Search(searchRequest)
	err := index.Close()
	if err != nil {
		return
	}
	var result []Dataset
	var data Dataset
	for _, v := range searchResult.Hits {
		data.ID = v.Fields["id"].(string)
		data.Name = v.Fields["name"].(string)
		data.Rank = v.Fields["rank"].(string)
		data.Industry = v.Fields["industry"].(string)
		data.HeadquartersState = v.Fields["headquarters_state"].(string)
		data.Revenues = v.Fields["revenues"].(string)
		data.Year = v.Fields["year"].(string)
		result = append(result, data)
	}
	if len(result) == 0 {
		c.IndentedJSON(200, gin.H{
			"code":    200,
			"message": "Empty result", // cast it to string before showing
		})
	} else {
		c.IndentedJSON(200, result)
	}
}

func main() {
	//dataset := createDataset()
	//indexing(dataset)
	router := gin.Default()
	router.POST("/query", postQuery)
	err := router.Run("localhost:8080")
	if err != nil {
		return
	}
}
