package main

import (
	"fmt"
	r "github.com/dancannon/gorethink"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	err              error
	gino             *gin.Engine
	rethinkSession   *r.Session
	analyticsChannel chan Change
	allSessions      map[*Session]bool
	changes          *r.Cursor
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		if r.Header.Get("Origin") != "http://"+r.Host {
			return false
		}
		return true
	},
}

type M map[string]interface{}

type Change struct {
	NewVal M `gorethink:"new_val"`
	OldVal M `gorethink:"old_val"`
}

type Session struct {
	Conn *websocket.Conn
}

type SocketOutgoingMessage struct {
	Function string                 `json:"function"`
	Data     map[string]interface{} `json:"data"`
}

func main() {
	rethinkSession, err = r.Connect(r.ConnectOpts{
		Address:  "localhost:28015",
		Database: "test",
	})
	if err != nil {
		log.Fatalf("Can't connect to rethinkdb: %q", err.Error())
	}

	_, err := r.DB("test").TableCreate("analytics").RunWrite(rethinkSession)
	if err != nil {
		log.Println(err)
	}

	//let's create the channel that will receive from the changefeed and pass the change objects to the connected websockets
	analyticsChannel = make(chan Change)

	allSessions = make(map[*Session]bool)

	changes, err = r.Table("analytics").Changes().Run(rethinkSession)
	if err != nil {
		log.Println(err)
	}
	defer changes.Close()

	//listen to changes and pass them to "analyticsChannel"
	changes.Listen(analyticsChannel)

	go func() {
		//this goroutine is an infinite loop that iterates over "analyticsChannel"
		for {
			var received Change

			if receivedTemp, ok := <-analyticsChannel; ok {
				received = receivedTemp
				log.Println("new received:", received)
			} else {
				log.Println("analyticsChannel is closed")
				break
			}

			if received.NewVal == nil || len(received.NewVal) == 0 {
				log.Println("discarded", received)
				continue
			}

			socketOutgoingMessage := &SocketOutgoingMessage{
				Function: "loadLatestSales", //this is the function (in the "window.analyticsNS" namespace) that will be triggered on the client side
				Data: M{
					"type": received.NewVal["type"],
					"data": received.NewVal["data"],
				},
			}

			log.Println("Connected sessions count:", len(allSessions))

			for session := range allSessions {
				log.Println("sending socketOutgoingMessage")
				err := session.Conn.WriteJSON(socketOutgoingMessage)
				if err != nil {
					log.Println(err)
					continue
				}
			}
		}
	}()

	gin.SetMode(gin.DebugMode)
	gino = gin.Default()

	gino.Static("/assets", "./assets")
	gino.StaticFile("/dashboard", "./dashboard.html")
	gino.StaticFile("/checkout", "./checkout.html")

	gino.POST("/checkout", func(c *gin.Context) {
		c.Writer.Header().Set("x-frame-options", "SAMEORIGIN")
		c.Writer.Header().Set("x-xss-protection", "1; mode=block")

		c.Request.ParseForm()

		//convert the "price" field string to float64
		price, err := strconv.ParseFloat(strings.TrimSpace(c.Request.Form.Get("price")), 64)
		if err != nil {
			log.Println(err)
			return
		}

		resp, err := r.Table("analytics").Insert(M{
			"type": "sale",
			"data": M{
				"buyer_id":   strings.TrimSpace(c.Request.Form.Get("buyer_id")),
				"product_id": strings.TrimSpace(c.Request.Form.Get("product_id")),
				"price":      price,
				"date":       r.Now(),
			},
		}).RunWrite(rethinkSession)

		if err != nil {
			log.Printf("error while inserting: %q", err)
		}
		log.Printf("%d event inserted\n", resp.Inserted)

		c.Data(200, "text/html", []byte("Success"))
	})

	gino.GET("/socket", func(c *gin.Context) {
		c.Writer.Header().Set("x-frame-options", "SAMEORIGIN")
		c.Writer.Header().Set("x-xss-protection", "1; mode=block")

		analyticsSocket(c.Writer, c.Request)
	})

	gino.Run(":8011") //127.0.0.1:8011
}

func analyticsSocket(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", 405)
		return
	}

	websocketConnection, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("upgrade:", err)
		return
	}
	log.Println("websocket successfully opened")

	var thisSession Session
	thisSession.Conn = websocketConnection
	allSessions[&thisSession] = true //some config we will never use in this example

	defer func() {
		websocketConnection.Close()
		delete(allSessions, &thisSession)
		log.Println("websocket closed")
	}()

	loadLatestSales(websocketConnection)
	loadProductStatsByNumberOfSales()
	loadProductStatsByRevenue()
	loadProductStats(websocketConnection)
	loadGraphData(websocketConnection)

	//let's setup an infinite loop that receives from the websocket to keep it running
	for {
		//in this example we don't need the received data, so we just discard it
		if _, _, err := websocketConnection.ReadMessage(); err != nil {
			websocketConnection.Close()
			break
		}
	}
}

func loadLatestSales(websocketConnection *websocket.Conn) {
	results, err := r.Table("analytics").Filter(M{
		"type": "sale",
	}).OrderBy(
		r.Desc(r.Row.Field("data").Field("date")),
	).Limit(10).Map(func(row r.Term) interface{} {
		return row.Field("data")
	}).Run(rethinkSession)
	if err != nil {
		log.Printf("error loadLatestSales: %q", err)
	}
	defer results.Close()

	var latestSales []interface{}
	err = results.All(&latestSales)
	if err != nil {
		log.Println(err)
		return
	}

	log.Printf("sending latest %d sales\n", len(latestSales))

	socketOutgoingMessage := &SocketOutgoingMessage{
		Function: "loadLatestSales",
		Data: M{
			"type": "sale",
			"data": latestSales,
		},
	}

	err = websocketConnection.WriteJSON(socketOutgoingMessage)
	if err != nil {
		log.Println(err)
	}
}

///all these are used for sorting by reduction; you can read more at https://golang.org/pkg/sort/
type Result struct {
	Group     string
	Reduction float64
}
type ByReduction []Result

func (a ByReduction) Len() int           { return len(a) }
func (a ByReduction) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByReduction) Less(i, j int) bool { return a[i].Reduction < a[j].Reduction } //sort in ASCENDING order
func (p Result) String() string {
	//if the number is an int, exclude the decimal part, otherwise include two decimal points
	if p.Reduction == float64(int64(p.Reduction)) {
		return fmt.Sprintf("%s: %.0f", p.Group, p.Reduction)
	} else {
		return fmt.Sprintf("%s: %.2f", p.Group, p.Reduction)
	}
}

func loadProductStatsByNumberOfSales() {
	results, err := r.Table("analytics").Filter(M{
		"type": "sale",
	}).Group(func(row r.Term) interface{} {
		return row.Field("data").Field("product_id")
	}).Count().Run(rethinkSession)

	if err != nil {
		log.Printf("error loadProductStatsByNumberOfSales: %q", err)
	}
	defer results.Close()

	var topSellingProductsByNumberOfSales []Result
	err = results.All(&topSellingProductsByNumberOfSales)
	if err != nil {
		log.Println(err)
		return
	}
	sort.Sort(ByReduction(topSellingProductsByNumberOfSales))

	log.Println(topSellingProductsByNumberOfSales)
}
func loadProductStatsByRevenue() {
	results, err := r.Table("analytics").Filter(M{
		"type": "sale",
	}).Group(func(row r.Term) interface{} {
		return row.Field("data").Field("product_id")
	}).Sum(func(row r.Term) interface{} {
		return row.Field("data").Field("price")
	}).Run(rethinkSession)

	if err != nil {
		log.Printf("error loadProductStatsByRevenue: %q", err)
	}
	defer results.Close()

	var topSellingProductsByRevenue []Result
	err = results.All(&topSellingProductsByRevenue)
	if err != nil {
		log.Println(err)
		return
	}
	sort.Sort(ByReduction(topSellingProductsByRevenue))

	log.Println(topSellingProductsByRevenue)
}

func loadProductStats(websocketConnection *websocket.Conn) {
	results, err := r.Table("analytics").Filter(M{
		"type": "sale",
	}).Group(func(row r.Term) interface{} {
		return row.Field("data").Field("product_id")
	}).Map(func(row r.Term) interface{} {
		return map[string]interface{}{
			"n_sales": 1,
			"revenue": row.Field("data").Field("price"),
		}
	}).Reduce(func(left, right r.Term) interface{} {
		return map[string]interface{}{
			"n_sales": left.Field("n_sales").Add(right.Field("n_sales")),
			"revenue": left.Field("revenue").Add(right.Field("revenue")),
		}
	}).Run(rethinkSession)

	if err != nil {
		log.Printf("error loadProductStats: %q", err)
	}
	defer results.Close()

	var topSellingProducts []interface{}
	err = results.All(&topSellingProducts)
	if err != nil {
		log.Println(err)
		return
	}

	log.Println("\nloadProductStats:", topSellingProducts)

	socketOutgoingMessage := &SocketOutgoingMessage{
		Function: "loadProductStats",
		Data: M{
			"data": topSellingProducts,
		},
	}
	err = websocketConnection.WriteJSON(socketOutgoingMessage)
	if err != nil {
		log.Println(err)
	}
}

func loadGraphData(websocketConnection *websocket.Conn) {
	currentTimestamp, _ := strconv.Atoi(strconv.FormatInt(time.Now().Unix(), 10))
	timeFrom := currentTimestamp - 60*60 //the stats for the last 60 minutes

	results, err := r.Table("analytics").Filter(
		r.Row.Field("type").Eq("sale").And(
			r.Row.Field("data").Field("date").Ge(r.EpochTime(timeFrom)),
			//exclude entries older than timeFrom
		),
	).Group(func(row r.Term) interface{} {
		return []interface{}{
			row.Field("data").Field("date").Hours(),
			row.Field("data").Field("date").Minutes(),
		}
	}).Map(func(row r.Term) interface{} {
		return map[string]interface{}{
			"n_sales": 1,
			"revenue": row.Field("data").Field("price"),
			"date":    row.Field("data").Field("date"),
		}
	}).Reduce(func(left, right r.Term) interface{} {
		return map[string]interface{}{
			"n_sales": left.Field("n_sales").Add(right.Field("n_sales")),
			"revenue": left.Field("revenue").Add(right.Field("revenue")),
			"date":    left.Field("date"),
		}
	}).Ungroup().Map(func(row r.Term) interface{} {
		return map[string]interface{}{
			"timeframe": row.Field("group"),
			"date":      row.Field("reduction").Field("date").ToEpochTime(),
			"reduction": map[string]interface{}{
				"n_sales":     row.Field("reduction").Field("n_sales"),
				"revenue":     row.Field("reduction").Field("revenue"),
				"avg_revenue": row.Field("reduction").Field("revenue").Div(row.Field("reduction").Field("n_sales")),

				//TODO:
				//number of unique users who bought a product
				//average spent per user
				//average n. of products bought per user
				//max expediture per user
				//the costliest product bought
				//repeat buys per product
			},
		}
	}).Run(rethinkSession)

	if err != nil {
		log.Printf("error loadGraphData: %q", err)
	}
	defer results.Close()

	var graphData []interface{}
	err = results.All(&graphData)
	if err != nil {
		log.Println(err)
		return
	}

	log.Println("\ngraph data:", graphData)

	socketOutgoingMessage := &SocketOutgoingMessage{
		Function: "loadGraphData",
		Data: M{
			"data": graphData,
		},
	}
	err = websocketConnection.WriteJSON(socketOutgoingMessage)
	if err != nil {
		log.Println(err)
	}
}
