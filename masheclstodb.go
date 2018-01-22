package main

import (
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/gorilla/websocket"
	_ "github.com/lib/pq"
	"github.com/op/go-logging"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"
)

const (
	dbhost = "blueskies.czstyhpil3qu.eu-west-2.rds.amazonaws.com"
	dbport = 5432
	dbuser = "tibco"
	dbpass = "tibco123"
	dbname = "mashlogs"
)

var (
	host       = flag.String("host", "streaming-api.mashery.com", "ECLS Service Host")
	path       = flag.String("path", "/ecls/subscribe/c47f06e6-2ef8-11e7-93ae-92361f002671/Acme", "ECLS Subscription Path")
	key        = flag.String("key", "vm6uYvgwt6rJDTevfUjZjT8WpEkzuaQmRTD", "API Key")
	psqlDbHost = flag.String("dbhost", dbhost, "PostgresSQL DB Hostname")
	psqlDbName = flag.String("dbname", dbname, "PostgresSQL DB Name")
	psqlDbPort = flag.Int("dbport", dbport, "PostgresSQL DB Port")
	psqlDBUser = flag.String("dbuser", dbuser, "PostgresSQL DB Username")
	psqlDBPass = flag.String("dbpass", dbpass, "PostgresSQL DB Password")

	db *sql.DB

	log    = logging.MustGetLogger("mdre")
	format = logging.MustStringFormatter(`%{color}%{time:15:04:05.000} %{shortfunc} ▶ %{level:.5s} %{color:reset} %{message}`)
)

type ECLS struct {
	Data []struct {
		APIKey                        string `json:"api_key"`
		APIMethodName                 string `json:"api_method_name"`
		Bytes                         string `json:"bytes"`
		CacheHit                      string `json:"cache_hit"`
		ClientTransferTime            string `json:"client_transfer_time"`
		ConnectTime                   string `json:"connect_time"`
		EndpointName                  string `json:"endpoint_name"`
		HTTPMethod                    string `json:"http_method"`
		HTTPStatusCode                string `json:"http_status_code"`
		HTTPVersion                   string `json:"http_version"`
		OauthAccessToken              string `json:"oauth_access_token"`
		PackageName                   string `json:"package_name"`
		PackageUUID                   string `json:"package_uuid"`
		PlanName                      string `json:"plan_name"`
		PlanUUID                      string `json:"plan_uuid"`
		PreTransferTime               string `json:"pre_transfer_time"`
		QPSThrottleValue              string `json:"qps_throttle_value"`
		QuotaValue                    string `json:"quota_value"`
		Referrer                      string `json:"referrer"`
		RemoteTotalTime               string `json:"remote_total_time"`
		RequestHostName               string `json:"request_host_name"`
		RequestID                     string `json:"request_id"`
		RequestTime                   string `json:"request_time"`
		RequestUUID                   string `json:"request_uuid"`
		ResponseString                string `json:"response_string"`
		ServiceDefinitionEndpointUUID string `json:"service_definition_endpoint_uuid"`
		ServiceID                     string `json:"service_id"`
		ServiceName                   string `json:"service_name"`
		SrcIP                         string `json:"src_ip"`
		SslEnabled                    string `json:"ssl_enabled"`
		TotalRequestExecTime          string `json:"total_request_exec_time"`
		TrafficManager                string `json:"traffic_manager"`
		TrafficManagerErrorCode       string `json:"traffic_manager_error_code"`
		URI                           string `json:"uri"`
		UserAgent                     string `json:"user_agent"`
	} `json:"data"`
}

func init() {

	backend1 := logging.NewLogBackend(os.Stderr, "", 0)
	backend2Formatter := logging.NewBackendFormatter(backend1, format)
	backend1Leveled := logging.AddModuleLevel(backend1)
	backend1Leveled.SetLevel(logging.ERROR, "")
	logging.SetBackend(backend1Leveled, backend2Formatter)

	log.Debug("init()")

}

func persistEventToDb(db *sql.DB, e ECLS) error {

	sqlClause := `INSERT INTO api_transactions (request_host_name,src_ip,request_uuid,http_method,uri,http_version,bytes,
		http_status_code,referrer,user_agent,request_id,request_time,api_key,service_id,traffic_manager,api_method_name,
		cache_hit,traffic_manager_error_code,total_request_exec_time,remote_total_time,connect_time,pre_transfer_time,
		oauth_access_token,ssl_enabled,quota_value,qps_throttle_value,client_transfer_time,service_name,response_string,
		plan_name,plan_uuid,endpoint_name,package_name,package_uuid,service_definition_endpoint_uuid)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,TO_TIMESTAMP ($12,'MM/DD/YYYY fmHH24fm:MI:SS.FF'),$13,
$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35);`


  requestHostName := e.Data[0].RequestHostName
	srcIP := e.Data[0].SrcIP
	requestUUID := e.Data[0].RequestUUID
	httpMethod := e.Data[0].HTTPMethod
	uri := e.Data[0].URI
	httpVersion := e.Data[0].HTTPVersion
	bytes := e.Data[0].Bytes
	httpStatusCode := e.Data[0].HTTPStatusCode
	referer := e.Data[0].Referrer
	userAgent := e.Data[0].UserAgent
	requestId := e.Data[0].RequestID
	requestTime := e.Data[0].RequestTime
	apiKey := e.Data[0].APIKey
	serviceId := e.Data[0].ServiceID
	trafficManager := e.Data[0].TrafficManager
	apiMethodName := e.Data[0].APIMethodName
	cacheHit := e.Data[0].CacheHit
	trafficManagerErrorCode := e.Data[0].TrafficManagerErrorCode
	totalRequestExecTime := e.Data[0].TotalRequestExecTime
	remoteTotalTime := e.Data[0].RemoteTotalTime
	connectTime := e.Data[0].ConnectTime
	preTransferTime := e.Data[0].PreTransferTime
	oathAccessToken := e.Data[0].OauthAccessToken
	sslEnabled := e.Data[0].SslEnabled
	quotaValue := e.Data[0].QuotaValue
	qpsThrottleValue := e.Data[0].QPSThrottleValue
	clientTransferTime := e.Data[0].ClientTransferTime
	serviceName := e.Data[0].ServiceName
	responseString := e.Data[0].ResponseString
	planName := e.Data[0].PlanName
	planUuid := e.Data[0].PlanUUID
	endpointName := e.Data[0].EndpointName
	packageName := e.Data[0].PackageName
	packageUuid := e.Data[0].PackageUUID
	serviceDefinitionEndpointUuid := e.Data[0].ServiceDefinitionEndpointUUID

	_, err := db.Exec(sqlClause, requestHostName, srcIP, requestUUID, httpMethod, uri, httpVersion, bytes,
		httpStatusCode, referer, userAgent, requestId, requestTime, apiKey, serviceId, trafficManager, apiMethodName,
			cacheHit, trafficManagerErrorCode, totalRequestExecTime, remoteTotalTime, connectTime, preTransferTime,
				oathAccessToken, sslEnabled, quotaValue, qpsThrottleValue, clientTransferTime, serviceName, responseString,
		planName, planUuid, endpointName, packageName, packageUuid, serviceDefinitionEndpointUuid)

	if err != nil {
		log.Error(err)
		return err
	}

	log.Debug("inserted row into api_transactions")

	return nil
}

func main() {

	flag.Parse()

	// Try and connect to PostresSQL
	dbinfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", dbhost, dbport, dbuser, dbpass, dbname)

	db, err := sql.Open("postgres", dbinfo)
	if err != nil {
		log.Error(err)
		panic(err)
	}

	defer db.Close()

	log.Debugf("connected to postgres: %s", dbinfo)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	var rawQuery string = "key="
	rawQuery += *key

	u := url.URL{Scheme: "wss", Host: *host, Path: *path, RawQuery: rawQuery}
	log.Debugf("connecting to %s", u.String())

	tlsConfig := &tls.Config{InsecureSkipVerify: true}

	d := websocket.Dialer{TLSClientConfig: tlsConfig, EnableCompression: true}

	c, resp, err := d.Dial(u.String(), nil)
	if err != nil {
		log.Debugf("handshake failed with status %d", resp.StatusCode)
		os.Exit(-1)
	}
	defer c.Close()

	done := make(chan struct{})

	go func() {
		defer c.Close()
		defer close(done)
		for {
			log.Debug("event processing cycle starting")
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Errorf("read:", err)
				return
			}

			log.Debug("received message from websocket")

			var s = string(message)
			if strings.HasPrefix(s, "Response To") {
				log.Debug("acknowledgement received")
			} else {
				log.Debug("event received")
				var e ECLS
				err = json.Unmarshal(message, &e)
				if err != nil {
					log.Errorf("unmarshal:", err)
				}

				err = persistEventToDb(db, e)
				if err != nil {
					log.Errorf("persist:", err)
				}
			}
			log.Debug("event processing cycle completed")
		}
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case t := <-ticker.C:
			err := c.WriteMessage(websocket.TextMessage, []byte(t.String()))
			if err != nil {
				log.Errorf("write:", err)
				return
			}
		case <-interrupt:
			log.Debug("interrupt")
			// To cleanly close a connection, a client should send a close
			// frame and wait for the server to close the connection.
			err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				log.Errorf("write close:", err)
				return
			}
			select {
			case <-done:
			case <-time.After(time.Second):
			}
			c.Close()
			return
		}
	}
}
