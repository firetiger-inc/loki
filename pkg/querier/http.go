package querier

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/grafana/loki/pkg/loghttp"
	"github.com/grafana/loki/pkg/logproto"
	"github.com/grafana/loki/pkg/logql"
	"github.com/grafana/loki/pkg/logql/marshal"
	marshal_legacy "github.com/grafana/loki/pkg/logql/marshal/legacy"
	serverutil "github.com/grafana/loki/pkg/util/server"
	"github.com/grafana/loki/pkg/util/validation"
)

const (
	wsPingPeriod = 1 * time.Second
)

type QueryResponse struct {
	ResultType parser.ValueType `json:"resultType"`
	Result     parser.Value     `json:"result"`
}

type QuerierAPI struct {
	cfg    Config
	engine *logql.Engine
	limits *validation.Overrides
	labels LabelQueryable
}

type LabelQueryable interface {
	// LabelQuerier returns a new Querier on the storage.
	LabelQuerier(mint, maxt int64) (LabelQuerier, error)
}

type LabelQuerier interface {
	Close() error
	// LabelValues returns all potential values for a label name in sorted order.
	// It is not safe to use the strings beyond the lifetime of the querier.
	// If matchers are specified the returned result set is reduced
	// to label values of metrics matching the matchers.
	LabelValues(ctx context.Context, name string, limit int, matchers ...*LabelMatcher) ([]string, error)
	// LabelNames returns all the unique label names present in the block in sorted order.
	// If matchers are specified the returned result set is reduced
	// to label names of metrics matching the matchers.
	LabelNames(ctx context.Context, limit int, matchers ...*LabelMatcher) ([]string, error)
}

type LabelMatcher struct {
	Type  LabelMatchType
	Name  string
	Value string
	// contains filtered or unexported fields
}

type LabelMatchType int

const (
	MatchEqual LabelMatchType = iota
	MatchNotEqual
	MatchRegexp
	MatchNotRegexp
)

func (q *Querier) RangeQueryHandler(w http.ResponseWriter, r *http.Request) {
	q.API().RangeQueryHandler(w, r)
}

// RangeQueryHandler is a http.HandlerFunc for range queries.
func (q *QuerierAPI) RangeQueryHandler(w http.ResponseWriter, r *http.Request) {
	// Enforce the query timeout while querying backends
	ctx, cancel := context.WithDeadline(r.Context(), time.Now().Add(q.cfg.QueryTimeout))
	defer cancel()

	request, err := loghttp.ParseRangeQuery(r)
	if err != nil {
		serverutil.WriteError(httpgrpc.Errorf(http.StatusBadRequest, err.Error()), w)
		return
	}

	if err := q.validateEntriesLimits(ctx, request.Limit); err != nil {
		serverutil.WriteError(err, w)
		return
	}

	params := logql.NewLiteralParams(
		request.Query,
		request.Start,
		request.End,
		request.Step,
		request.Interval,
		request.Direction,
		request.Limit,
		request.Shards,
	)
	query := q.engine.Query(params)
	result, err := query.Exec(ctx)
	if err != nil {
		serverutil.WriteError(err, w)
		return
	}

	if err := marshal.WriteQueryResponseJSON(result, w); err != nil {
		serverutil.WriteError(err, w)
		return
	}
}

func (q *Querier) InstantQueryHandler(w http.ResponseWriter, r *http.Request) {
	q.API().InstantQueryHandler(w, r)
}

// InstantQueryHandler is a http.HandlerFunc for instant queries.
func (q *QuerierAPI) InstantQueryHandler(w http.ResponseWriter, r *http.Request) {
	// Enforce the query timeout while querying backends
	ctx, cancel := context.WithDeadline(r.Context(), time.Now().Add(q.cfg.QueryTimeout))
	defer cancel()

	request, err := loghttp.ParseInstantQuery(r)
	if err != nil {
		serverutil.WriteError(httpgrpc.Errorf(http.StatusBadRequest, err.Error()), w)
		return
	}

	if err := q.validateEntriesLimits(ctx, request.Limit); err != nil {
		serverutil.WriteError(err, w)
		return
	}

	params := logql.NewLiteralParams(
		request.Query,
		request.Ts,
		request.Ts,
		0,
		0,
		request.Direction,
		request.Limit,
		nil,
	)
	query := q.engine.Query(params)
	result, err := query.Exec(ctx)
	if err != nil {
		serverutil.WriteError(err, w)
		return
	}

	if err := marshal.WriteQueryResponseJSON(result, w); err != nil {
		serverutil.WriteError(err, w)
		return
	}
}

func (q *Querier) LogQueryHandler(w http.ResponseWriter, r *http.Request) {
	q.API().LogQueryHandler(w, r)
}

// LogQueryHandler is a http.HandlerFunc for log only queries.
func (q *QuerierAPI) LogQueryHandler(w http.ResponseWriter, r *http.Request) {
	// Enforce the query timeout while querying backends
	ctx, cancel := context.WithDeadline(r.Context(), time.Now().Add(q.cfg.QueryTimeout))
	defer cancel()

	request, err := loghttp.ParseRangeQuery(r)
	if err != nil {
		serverutil.WriteError(httpgrpc.Errorf(http.StatusBadRequest, err.Error()), w)
		return
	}
	request.Query, err = parseRegexQuery(r)
	if err != nil {
		serverutil.WriteError(httpgrpc.Errorf(http.StatusBadRequest, err.Error()), w)
		return
	}

	expr, err := logql.ParseExpr(request.Query)
	if err != nil {
		serverutil.WriteError(err, w)
		return
	}

	// short circuit metric queries
	if _, ok := expr.(logql.SampleExpr); ok {
		serverutil.WriteError(httpgrpc.Errorf(http.StatusBadRequest, "legacy endpoints only support %s result type", logql.ValueTypeStreams), w)
		return
	}

	if err := q.validateEntriesLimits(ctx, request.Limit); err != nil {
		serverutil.WriteError(err, w)
		return
	}

	params := logql.NewLiteralParams(
		request.Query,
		request.Start,
		request.End,
		request.Step,
		request.Interval,
		request.Direction,
		request.Limit,
		request.Shards,
	)
	query := q.engine.Query(params)

	result, err := query.Exec(ctx)
	if err != nil {
		serverutil.WriteError(err, w)
		return
	}

	if err := marshal_legacy.WriteQueryResponseJSON(result, w); err != nil {
		serverutil.WriteError(err, w)
		return
	}
}

func (q *Querier) LabelHandler(w http.ResponseWriter, r *http.Request) {
	q.API().LabelHandler(w, r)
}

// LabelHandler is a http.HandlerFunc for handling label queries.
func (q *QuerierAPI) LabelHandler(w http.ResponseWriter, r *http.Request) {
	req, err := loghttp.ParseLabelQuery(r)
	if err != nil {
		serverutil.WriteError(httpgrpc.Errorf(http.StatusBadRequest, err.Error()), w)
		return
	}

	resp, err := q.Label(r.Context(), req)
	if err != nil {
		serverutil.WriteError(err, w)
		return
	}

	if loghttp.GetVersion(r.RequestURI) == loghttp.VersionV1 {
		err = marshal.WriteLabelResponseJSON(*resp, w)
	} else {
		err = marshal_legacy.WriteLabelResponseJSON(*resp, w)
	}
	if err != nil {
		serverutil.WriteError(err, w)
		return
	}
}

func (q *Querier) TailHandler(w http.ResponseWriter, r *http.Request) {
	q.API().TailHandler(w, r)
}

// TailHandler is a http.HandlerFunc for handling tail queries.
func (q *QuerierAPI) TailHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "tail is not implemented yet", http.StatusNotImplemented)

	// upgrader := websocket.Upgrader{
	// 	CheckOrigin: func(r *http.Request) bool { return true },
	// }
	// logger := util.WithContext(r.Context(), util.Logger)

	// req, err := loghttp.ParseTailQuery(r)
	// if err != nil {
	// 	serverutil.WriteError(httpgrpc.Errorf(http.StatusBadRequest, err.Error()), w)
	// 	return
	// }

	// req.Query, err = parseRegexQuery(r)
	// if err != nil {
	// 	serverutil.WriteError(httpgrpc.Errorf(http.StatusBadRequest, err.Error()), w)
	// 	return
	// }

	// conn, err := upgrader.Upgrade(w, r, nil)
	// if err != nil {
	// 	level.Error(logger).Log("msg", "Error in upgrading websocket", "err", err)
	// 	return
	// }

	// defer func() {
	// 	if err := conn.Close(); err != nil {
	// 		level.Error(logger).Log("msg", "Error closing websocket", "err", err)
	// 	}
	// }()

	// tailer, err := q.Tail(r.Context(), req)
	// if err != nil {
	// 	if err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, err.Error())); err != nil {
	// 		level.Error(logger).Log("msg", "Error connecting to ingesters for tailing", "err", err)
	// 	}
	// 	return
	// }
	// defer func() {
	// 	if err := tailer.close(); err != nil {
	// 		level.Error(logger).Log("msg", "Error closing Tailer", "err", err)
	// 	}
	// }()

	// ticker := time.NewTicker(wsPingPeriod)
	// defer ticker.Stop()

	// var response *loghttp_legacy.TailResponse
	// responseChan := tailer.getResponseChan()
	// closeErrChan := tailer.getCloseErrorChan()

	// doneChan := make(chan struct{})
	// go func() {
	// 	for {
	// 		_, _, err := conn.ReadMessage()
	// 		if err != nil {
	// 			if closeErr, ok := err.(*websocket.CloseError); ok {
	// 				if closeErr.Code == websocket.CloseNormalClosure {
	// 					break
	// 				}
	// 				level.Error(logger).Log("msg", "Error from client", "err", err)
	// 				break
	// 			} else if tailer.stopped {
	// 				return
	// 			} else {
	// 				level.Error(logger).Log("msg", "Unexpected error from client", "err", err)
	// 				break
	// 			}
	// 		}
	// 	}
	// 	doneChan <- struct{}{}
	// }()

	// for {
	// 	select {
	// 	case response = <-responseChan:
	// 		var err error
	// 		if loghttp.GetVersion(r.RequestURI) == loghttp.VersionV1 {
	// 			err = marshal.WriteTailResponseJSON(*response, conn)
	// 		} else {
	// 			err = marshal_legacy.WriteTailResponseJSON(*response, conn)
	// 		}
	// 		if err != nil {
	// 			level.Error(logger).Log("msg", "Error writing to websocket", "err", err)
	// 			if err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, err.Error())); err != nil {
	// 				level.Error(logger).Log("msg", "Error writing close message to websocket", "err", err)
	// 			}
	// 			return
	// 		}

	// 	case err := <-closeErrChan:
	// 		level.Error(logger).Log("msg", "Error from iterator", "err", err)
	// 		if err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, err.Error())); err != nil {
	// 			level.Error(logger).Log("msg", "Error writing close message to websocket", "err", err)
	// 		}
	// 		return
	// 	case <-ticker.C:
	// 		// This is to periodically check whether connection is active, useful to clean up dead connections when there are no entries to send
	// 		if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
	// 			level.Error(logger).Log("msg", "Error writing ping message to websocket", "err", err)
	// 			if err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, err.Error())); err != nil {
	// 				level.Error(logger).Log("msg", "Error writing close message to websocket", "err", err)
	// 			}
	// 			return
	// 		}
	// 	case <-doneChan:
	// 		return
	// 	}
	// }
}

func (q *Querier) SeriesHandler(w http.ResponseWriter, r *http.Request) {
	q.API().SeriesHandler(w, r)
}

// SeriesHandler returns the list of time series that match a certain label set.
// See https://prometheus.io/docs/prometheus/latest/querying/api/#finding-series-by-label-matchers
func (q *QuerierAPI) SeriesHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "series is not implemented yet", http.StatusNotImplemented)
	// req, err := loghttp.ParseSeriesQuery(r)
	// if err != nil {
	// 	serverutil.WriteError(httpgrpc.Errorf(http.StatusBadRequest, err.Error()), w)
	// 	return
	// }

	// resp, err := q.Series(r.Context(), req)
	// if err != nil {
	// 	serverutil.WriteError(err, w)
	// 	return
	// }

	// err = marshal.WriteSeriesResponseJSON(*resp, w)
	// if err != nil {
	// 	serverutil.WriteError(err, w)
	// 	return
	// }
}

// parseRegexQuery parses regex and query querystring from httpRequest and returns the combined LogQL query.
// This is used only to keep regexp query string support until it gets fully deprecated.
func parseRegexQuery(httpRequest *http.Request) (string, error) {
	query := httpRequest.Form.Get("query")
	regexp := httpRequest.Form.Get("regexp")
	if regexp != "" {
		expr, err := logql.ParseLogSelector(query)
		if err != nil {
			return "", err
		}
		query = logql.NewFilterExpr(expr, labels.MatchRegexp, regexp).String()
	}
	return query, nil
}

func (q *QuerierAPI) validateEntriesLimits(ctx context.Context, limit uint32) error {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return httpgrpc.Errorf(http.StatusBadRequest, err.Error())
	}

	maxEntriesLimit := q.limits.MaxEntriesLimitPerQuery(userID)
	if int(limit) > maxEntriesLimit && maxEntriesLimit != 0 {
		return httpgrpc.Errorf(http.StatusBadRequest,
			"max entries limit per query exceeded, limit > max_entries_limit (%d > %d)", limit, maxEntriesLimit)
	}
	return nil
}

// Label does the heavy lifting for a Label query.
func (q *QuerierAPI) Label(ctx context.Context, req *logproto.LabelRequest) (*logproto.LabelResponse, error) {
	// Enforce the query timeout while querying backends
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(q.cfg.QueryTimeout))
	defer cancel()

	// resps, err := q.forAllIngesters(ctx, func(client logproto.QuerierClient) (interface{}, error) {
	// 	return client.Label(ctx, req)
	// })
	// if err != nil {
	// 	return nil, err
	// }

	// userID, err := user.ExtractOrgID(ctx)
	// if err != nil {
	// 	return nil, err
	// }

	from, through := req.Start.UnixNano(), req.End.UnixNano()
	querier, err := q.labels.LabelQuerier(from, through)
	if err != nil {
		return nil, err
	}
	defer querier.Close()

	var values []string
	if req.Values {
		values, err = querier.LabelValues(ctx, req.Name, -1 /* matchers */)
	} else {
		values, err = querier.LabelNames(ctx, -1 /* matchers */)
	}
	if err != nil {
		return nil, err
	}

	return &logproto.LabelResponse{Values: values}, nil
}
