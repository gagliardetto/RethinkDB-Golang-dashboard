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
	err               error
	gino              *gin.Engine
	rethinkSession    *r.Session
	analyticsChannel  chan Change
	allSocketSessions map[*AnalyticsSession]bool
	changes           *r.Cursor
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

type AnalyticsSession struct {
	Request *http.Request
	Conn    *websocket.Conn
}

type SocketOutgoingMessage struct {
	Function string                 `json:"function"`
	Data     map[string]interface{} `json:"data"`
}

func main() {
	//let's connect to the db and use the "test" database
	rethinkSession, err = r.Connect(r.ConnectOpts{
		Address:  "localhost:28015",
		Database: "test",
	})
	if err != nil {
		log.Fatalf("Can't connect to rethinkdb: %q", err.Error())
	}

	//let's create the "analytics" table on the "test" db
	_, err := r.DB("test").TableCreate("analytics").RunWrite(rethinkSession)
	if err != nil {
		//if the table already exists, the existing table won't be overwritten but an error will be thrown
		fmt.Println(err)
	}
	///

	//let's make the channel (declared globally) that will receive from the changefeed and pass the change objects to the connected websockets
	analyticsChannel = make(chan Change)

	//let's make the map which will contain all the cursors of websocket sessions
	allSocketSessions = make(map[*AnalyticsSession]bool)

	//let's initiate the changefeed, which will feed to the "analyticsChannel" channel all the change objects
	changes, err = r.Table("analytics").Changes().Run(rethinkSession)
	if err != nil {
		log.Println(err)
	}
	defer changes.Close()

	//listen to changes and pass them to "analyticsChannel"
	changes.Listen(analyticsChannel)
	///

	go func() {
		//this goroutine is an infinite loop that receives from "analyticsChannel"
		for {
			//"received" is of "Change" type
			var received Change

			if rTemp, ok := <-analyticsChannel; ok {
				//"analyticsChannel" is open and has received a new object
				received = rTemp
				fmt.Println("ok")
			} else {
				//"analyticsChannel" is closed
				fmt.Println("channel is closed")
				//the channel has been closed intentionally, or the connection to the server went down, or some other reason.

				//break the receiving for loop
				break
			}

			//check for nil or empty "received" objects
			if received.NewVal == nil || len(received.NewVal) == 0 {
				fmt.Println("discarded", received)
				continue
			}
			//warning: along with new entries to "analytics", modified entries will be catched too.

			fmt.Println("new received:", received)

			//create a new "socketOutgoingMessage"
			socketOutgoingMessage := &SocketOutgoingMessage{
				Function: "switch", //this is the function (in the "window.grapherF" namespace) that will be triggered on the client side
				Data: M{
					"type": received.NewVal["type"], //type of event (sale|...)
					"data": received.NewVal["data"], //content of the event object
				},
			}

			//number of websockets currently connected and waiting for updates
			fmt.Println("sessions' #:", len(allSocketSessions))

			//iterate over all the connected websockets and send the "socketOutgoingMessage"
			for s, v := range allSocketSessions {
				//fmt.Println(s)
				fmt.Println("config:", v)

				//send the "socketOutgoingMessage" in JSON format using the builtin encoder
				fmt.Println("sending change object")
				err := s.Conn.WriteJSON(socketOutgoingMessage)
				if err != nil {
					log.Println("write:", err)
					continue
				}
			}
		}
	}()

	//in this example we use the Gin web framework; you can use any framework you like
	gin.SetMode(gin.DebugMode)
	gino = gin.Default()
	gino.Static("/assets", "./assets") //folder containing json and css files

	gino.StaticFile("/dashboard", "./dashboard.html") //the page where websockets receive data and display it

	gino.StaticFile("/checkout", "./checkout.html") //a simple checkout page in html

	gino.POST("/checkout", func(c *gin.Context) {
		//the checkout POST endpoint

		//some headers for a bit of security
		c.Writer.Header().Set("x-frame-options", "SAMEORIGIN")
		c.Writer.Header().Set("x-xss-protection", "1; mode=block")

		c.Request.ParseForm() //parse the form

		//convert the "price" field to float64 format
		price, err := strconv.ParseFloat(strings.TrimSpace(c.Request.Form.Get("price")), 64)
		if err != nil {
			fmt.Println(err)
			return
		}

		//insert the purchase event to the "analytics" table
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
			fmt.Printf("error while inserting: %q", err)
		}
		fmt.Printf("%d event inserted\n", resp.Inserted)

		c.Data(200, "text/html", []byte("Success"))
	})

	gino.GET("/socket", func(c *gin.Context) {
		//this is the websocket endpoint

		c.Writer.Header().Set("x-frame-options", "SAMEORIGIN")
		c.Writer.Header().Set("x-xss-protection", "1; mode=block")

		/*
			write here the code that checks if user is logged in
		*/

		//The analyticsSocket function upgrades the connection.
		//Whichever web framework you use, pass it the http.ResponseWriter and *http.Request
		analyticsSocket(c.Writer, c.Request)
	})

	gino.Run(":8011") // listen and serve on 127.0.0.1:8011
}

func analyticsSocket(w http.ResponseWriter, r *http.Request) {
	//let's check the method
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", 405)
		return
	}

	//upgrade to websocket
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("upgrade:", err)
		return
	}
	fmt.Println("websocket successfully opened")

	//greate a new "AnalyticsSession" object; this will be then added to "allSocketSessions"
	var thisSession AnalyticsSession
	thisSession.Conn = c                   //c is of *websocket.Conn type; it will be used to send messages
	thisSession.Request = r                //this is the request the client sent to the server; you can use it to read cookies, URL parameters, etc.
	allSocketSessions[&thisSession] = true //some config we will never use in this example

	//this will be fired when closing the connection
	defer func() {
		fmt.Println("websocket closed")
		c.Close()                               //close websocket
		delete(allSocketSessions, &thisSession) //remove this session from "allSocketSessions"
	}()

	///
	loadLatestSales(c)
	loadTopSellingProductsByNumberOfSales(c)
	loadTopSellingProductsByRevenue(c)
	loadTopSellingProducts(c)
	loadGraphData(c)
	///

	//let's setup an infiniteloop that receives from the websocket to keep it running
	for {
		//in this example we don't need the received data, so we just discard it
		if _, _, err := c.ReadMessage(); err != nil {
			c.Close()
			break
		}
	}
}

func loadLatestSales(c *websocket.Conn) {
	//this function loads the last n sales (the value inside Limit()) and sends them through the websocket

	results, err := r.Table("analytics").Filter(M{
		"type": "sale",
	}).OrderBy(
		r.Desc(r.Row.Field("data").Field("date")), //order by date from most recent to older
	).Limit(10).Map(func(row r.Term) interface{} {
		return row.Field("data")
	}).Run(rethinkSession)
	if err != nil {
		fmt.Printf("error loadLatestSales: %q", err)
	}
	defer results.Close()

	var latestSales []interface{}
	err = results.All(&latestSales)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("%d elements\n", len(latestSales))

	socketOutgoingMessage := &SocketOutgoingMessage{
		Function: "switch",
		Data: M{
			"type": "sale",
			"data": latestSales,
		},
	}

	//send latest sales through the websocket
	err = c.WriteJSON(socketOutgoingMessage)
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

///

func loadTopSellingProductsByNumberOfSales(c *websocket.Conn) {
	//all products by number of sales
	results, err := r.Table("analytics").Filter(M{
		"type": "sale",
	}).Group(func(row r.Term) interface{} {
		//group sales by product id
		return row.Field("data").Field("product_id")
	}).Count().Run(rethinkSession) //for each group (i.e. product id), count the sales

	if err != nil {
		fmt.Printf("error loadTopSellingProductsByNumberOfSales: %q", err)
	}
	defer results.Close()

	var topSellingProductsByNumberOfSales []Result
	err = results.All(&topSellingProductsByNumberOfSales)
	if err != nil {
		fmt.Println(err)
		return
	}
	sort.Sort(ByReduction(topSellingProductsByNumberOfSales)) //sort in ASCENDING order by number of sales ("reduction")

	fmt.Println(topSellingProductsByNumberOfSales)
}
func loadTopSellingProductsByRevenue(c *websocket.Conn) {
	//all products by dollars sold
	results, err := r.Table("analytics").Filter(M{
		"type": "sale",
	}).Group(func(row r.Term) interface{} {
		return row.Field("data").Field("product_id")
	}).Sum(func(row r.Term) interface{} {
		return row.Field("data").Field("price")
	}).Run(rethinkSession)

	if err != nil {
		fmt.Printf("error loadTopSellingProductsByRevenue: %q", err)
	}
	defer results.Close()

	var topSellingProductsByRevenue []Result
	err = results.All(&topSellingProductsByRevenue)
	if err != nil {
		fmt.Println(err)
		return
	}
	sort.Sort(ByReduction(topSellingProductsByRevenue))

	fmt.Println(topSellingProductsByRevenue)
}

func loadTopSellingProducts(c *websocket.Conn) {
	//all products by number of sales, and revenue
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
		fmt.Printf("error loadTopSellingProducts: %q", err)
	}
	defer results.Close()

	var topSellingProducts []interface{}
	err = results.All(&topSellingProducts)
	if err != nil {
		fmt.Println(err)
		return
	}

	socketOutgoingMessage := &SocketOutgoingMessage{
		Function: "loadTopSellingProducts",
		Data: M{
			"data": topSellingProducts,
		},
	}
	err = c.WriteJSON(socketOutgoingMessage)
	if err != nil {
		log.Println(err)
	}

	fmt.Println("\nloadTopSellingProducts:", topSellingProducts)
}

func loadGraphData(c *websocket.Conn) {
	currentTimestamp, _ := strconv.Atoi(strconv.FormatInt(time.Now().Unix(), 10))
	timeFrom := currentTimestamp - 60*60 //the stats for the last 60 minutes

	results, err := r.Table("analytics").Filter(
		r.Row.Field("type").Eq("sale").And(
			//we want entries of the past 60 minutes only
			r.Row.Field("data").Field("date").Ge(r.EpochTime(timeFrom)),
			//any entry older than timeFrom won't be considered
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
		fmt.Printf("error loadGraphData: %q", err)
	}
	defer results.Close()

	var graphData []interface{}
	err = results.All(&graphData)
	if err != nil {
		fmt.Println(err)
		return
	}

	socketOutgoingMessage := &SocketOutgoingMessage{
		Function: "loadGraphData",
		Data: M{
			"data": graphData,
		},
	}
	err = c.WriteJSON(socketOutgoingMessage)
	if err != nil {
		log.Println(err)
	}

	fmt.Println("\ngraph data:", graphData)
}
