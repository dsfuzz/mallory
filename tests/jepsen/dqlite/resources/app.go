package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/driver"
)

const (
	port   = 8080 // This is the API port, the internal dqlite port+1
	schema = "CREATE TABLE IF NOT EXISTS map (key INT, value INT)"
)

func dqliteLog(l client.LogLevel, format string, a ...interface{}) {
	log.Printf(fmt.Sprintf("%s: %s\n", l.String(), format), a...)
}

func makeAddress(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}

// Return the dqlite addresses of all nodes preceeding the given one.
//
// E.g. with node="n2" and cluster="n1,n2,n3" return ["n1:8081"]
func preceedingAddresses(node string, nodes []string) []string {
	preceeding := []string{}
	for i, name := range nodes {
		if name != node {
			continue
		}
		for j := 0; j < i; j++ {
			preceeding = append(preceeding, makeAddress(nodes[j], port+1))
		}
		break
	}
	return preceeding
}

// Return the dqlite addresses of all nodes different from the given one.
func otherAddresses(node string, nodes []string) []string {
	others := []string{}
	for _, name := range nodes {
		if name == node {
			continue
		}
		others = append(others, makeAddress(name, port+1))

	}
	return others
}

func withTx(db *sql.DB, f func(tx *sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	if err := f(tx); err != nil {
		tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

// Probe all given nodes for connectivity and metadata, then return a
// RolesChanges object.
//
// Adapted from the private method of the same name in go-dqlite/app.
func makeRolesChanges(a *app.App, nodes []client.NodeInfo, roles app.RolesConfig) app.RolesChanges {
	state := map[client.NodeInfo]*client.NodeMetadata{}
	for _, node := range nodes {
		state[node] = nil
	}

	var (
		mtx     sync.Mutex     // Protects state map
		wg      sync.WaitGroup // Wait for all probes to finish
		nProbes = runtime.NumCPU()
		sem     = semaphore.NewWeighted(int64(nProbes)) // Limit number of parallel probes
	)

	for _, node := range nodes {
		wg.Add(1)
		// sem.Acquire will not block forever because the goroutines
		// that release the semaphore will eventually timeout.
		if err := sem.Acquire(context.Background(), 1); err != nil {
			log.Printf("failed to acquire semaphore: %v", err)
			wg.Done()
			continue
		}
		go func(node client.NodeInfo) {
			defer wg.Done()
			defer sem.Release(1)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			cli, err := client.New(ctx, node.Address, client.WithLogFunc(dqliteLog))
			if err == nil {
				metadata, err := cli.Describe(ctx)
				if err == nil {
					mtx.Lock()
					state[node] = metadata
					mtx.Unlock()
				}
				cli.Close()
			}
		}(node)
	}

	wg.Wait()
	return app.RolesChanges{Config: roles, State: state}
}

func appendPost(ctx context.Context, db *sql.DB, value string) (string, error) {
	result := "["

	err := withTx(db, func(tx *sql.Tx) error {
		if value[0] != '[' || value[len(value)-1] != ']' {
			return fmt.Errorf("bad request")
		}

		value = value[1 : len(value)-1]
		for {
			if value[0] != '[' {
				return fmt.Errorf("bad request")
			}
			value = value[1:]
			i := strings.Index(value, "]")
			if i == -1 {
				return fmt.Errorf("bad request")
			}
			op := strings.Split(value[:i], " ")
			if len(op) != 3 {
				return fmt.Errorf("bad request")
			}

			switch op[0] {
			case ":r":
				values := []int{}
				if op[2] != "nil" {
					return fmt.Errorf("bad request")
				}

				rows, err := tx.QueryContext(ctx, "SELECT value FROM map WHERE key=?", op[1])
				if err != nil {
					return err
				}
				defer rows.Close()

				for i := 0; rows.Next(); i++ {
					var val int
					err := rows.Scan(&val)
					if err != nil {
						return err
					}
					values = append(values, val)
				}
				err = rows.Err()
				if err != nil {
					return err
				}

				result += fmt.Sprintf("[:r %s %v]", op[1], values)
			case ":append":
				if _, err := tx.ExecContext(
					ctx, "INSERT INTO map(key, value) VALUES(?, ?)",
					op[1], op[2]); err != nil {
					return err
				}
				result += fmt.Sprintf("[:append %s %s]", op[1], op[2])
			default:
				return fmt.Errorf("bad request")
			}

			if i == len(value)-1 {
				result += "]"
				break
			}

			value = value[i+2:]
			result += " "
		}
		return nil
	})

	if err != nil {
		return "", err
	}

	return result, nil
}

func bankGet(ctx context.Context, db *sql.DB) (string, error) {
	rows, err := db.QueryContext(ctx, "SELECT key, value FROM map")
	if err != nil {
		return "", err
	}
	defer rows.Close()

	accounts := []string{}
	for i := 0; rows.Next(); i++ {
		var id int
		var balance int
		err := rows.Scan(&id, &balance)
		if err != nil {
			return "", err
		}
		accounts = append(accounts, fmt.Sprintf("%d %d", id, balance))
	}
	err = rows.Err()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("{%s}", strings.Join(accounts, ", ")), nil
}

func bankPut(ctx context.Context, db *sql.DB, value string) (string, error) {
	accounts := []int{}
	total := 0
	value = value[1 : len(value)-1]
	for _, item := range strings.Split(value, ", ") {
		parts := strings.SplitN(item, " ", 2)
		k := parts[0]
		v := parts[1]
		switch k {
		case ":accounts":
			for _, id := range strings.Split(v[1:len(v)-1], " ") {
				id, _ := strconv.Atoi(id)
				accounts = append(accounts, id)
			}
		case ":total-amount":
			total, _ = strconv.Atoi(v)
		}
	}
	balance := total / len(accounts)
	err := withTx(db, func(tx *sql.Tx) error {
		var count int
		row := tx.QueryRowContext(ctx, "SELECT count(*) FROM map")
		if err := row.Scan(&count); err != nil {
			return err
		}
		if count > 0 {
			return nil
		}
		for _, id := range accounts {
			if _, err := tx.ExecContext(
				ctx, "INSERT INTO map(key, value) VALUES(?, ?)", id, balance); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		// TODO: retry instead.
		if err, ok := err.(driver.Error); !ok || (err.Code != driver.ErrBusy && err.Code != driver.ErrBusySnapshot) {
			return "", err
		}
	}

	return "nil", nil
}

func bankPost(ctx context.Context, db *sql.DB, value string) (string, error) {
	var from int
	var to int
	var amount int

	value = value[1 : len(value)-1]
	for _, item := range strings.Split(value, ", ") {
		parts := strings.SplitN(item, " ", 2)
		k := parts[0]
		v := parts[1]
		switch k {
		case ":from":
			from, _ = strconv.Atoi(v)
		case ":to":
			to, _ = strconv.Atoi(v)
		case ":amount":
			amount, _ = strconv.Atoi(v)
		}
	}

	err := withTx(db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(
			ctx, "UPDATE map SET value = value - ? WHERE key = ?", amount, from); err != nil {
			return err
		}
		if _, err := tx.ExecContext(
			ctx, "UPDATE map SET value = value + ? WHERE key = ?", amount, to); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	return "nil", nil
}

func setGet(ctx context.Context, db *sql.DB) (string, error) {
	rows, err := db.QueryContext(ctx, "SELECT value FROM map")
	if err != nil {
		return "", err
	}
	defer rows.Close()

	vals := []string{}
	for i := 0; rows.Next(); i++ {
		var val string
		err := rows.Scan(&val)
		if err != nil {
			return "", err
		}
		vals = append(vals, val)
	}
	err = rows.Err()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("[%s]", strings.Join(vals, " ")), nil
}

func setPost(ctx context.Context, db *sql.DB, value string) (string, error) {
	if _, err := db.ExecContext(ctx, "INSERT INTO map(value) VALUES(?)", value); err != nil {
		return "", err
	}
	return value, nil
}

func leaderGet(ctx context.Context, app *app.App) (string, error) {
	cli, err := app.Leader(ctx)
	if err != nil {
		return "", err
	}
	defer cli.Close()

	node, err := cli.Leader(ctx)
	if err != nil {
		return "", err
	}

	leader := ""
	if node != nil {
		addr := strings.Split(node.Address, ":")[0]
		hosts, err := net.LookupAddr(addr)
		if err != nil {
			return "", fmt.Errorf("%q: %v", node.Address, err)
		}
		if len(hosts) != 1 {
			return "", fmt.Errorf("more than one host associated with %s: %v", node.Address, hosts)
		}
		leader = hosts[0]
	}

	return fmt.Sprintf("\"%s\")", leader), nil
}

func membersGet(ctx context.Context, app *app.App) (string, error) {
	cli, err := app.Leader(ctx)
	if err != nil {
		return "", err
	}
	defer cli.Close()

	cluster, err := cli.Cluster(ctx)
	if err != nil {
		return "", err
	}

	nodes := make([]string, len(cluster))
	for i, node := range cluster {
		addr := strings.Split(node.Address, ":")[0]
		hosts, err := net.LookupAddr(addr)
		if err != nil {
			return "", fmt.Errorf("%q: %v", node.Address, err)
		}
		if len(hosts) != 1 {
			return "", fmt.Errorf("more than one host associated with %s: %v", node.Address, hosts)
		}
		nodes[i] = fmt.Sprintf("\"%s\"", hosts[0])
	}

	return fmt.Sprintf("[%s]", strings.Join(nodes, " ")), nil
}

func membersDelete(ctx context.Context, app *app.App, value string) (string, error) {
	cli, err := app.Leader(ctx)
	if err != nil {
		return "", err
	}
	defer cli.Close()

	cluster, err := cli.Cluster(ctx)
	if err != nil {
		return "", err
	}

	addr, err := net.ResolveIPAddr("ip", value)
	if err != nil {
		return "", err
	}

	address := makeAddress(addr.IP.String(), 8081)
	for _, node := range cluster {
		if node.Address == address {
			if err := cli.Remove(ctx, node.ID); err != nil {
				return "", err
			}
			return "nil", nil
		}
	}

	return "", fmt.Errorf("no node named %s", value)
}

func readyGet(ctx context.Context, app *app.App, nodes []string) (string, error) {
	cli, err := app.Leader(ctx)
	if err != nil {
		return "", err
	}
	defer cli.Close()

	cluster, err := cli.Cluster(ctx)
	if err != nil {
		return "", err
	}

	if n := len(cluster); n != len(nodes) {
		return "", fmt.Errorf("cluster has still only %d nodes", n)
	}

	for _, node := range cluster {
		if node.Role == client.Spare {
			return "", fmt.Errorf("node %s is still %s", node.Address, node.Role)
		}
	}

	return "nil", nil
}

func stableGet(ctx context.Context, app *app.App, nodes []string, roles app.RolesConfig, checkHealth bool) (string, error) {
	cli, err := app.Leader(ctx)
	if err != nil {
		return "", err
	}
	cluster, err := cli.Cluster(ctx)
	if err != nil {
		return "", err
	}
	changes := makeRolesChanges(app, cluster, roles)

	bad := false

	// Check whether the numbers of online nodes assigned each role make sense.
	numOnlineSpares := 0
	numOnlineStandbys := 0
	numOnlineVoters := 0
	offlines := []client.NodeInfo{}
	for node, meta := range changes.State {
		if meta != nil {
			switch node.Role {
			case client.Spare:
				numOnlineSpares += 1
			case client.StandBy:
				numOnlineStandbys += 1
			case client.Voter:
				numOnlineVoters += 1
			}
		} else {
			offlines = append(offlines, node)
		}
	}
	if checkHealth && len(offlines) != 0 {
		log.Printf("for jepsen: cluster is not healthy")
		bad = true
	}
	if numOnlineSpares > 0 && numOnlineStandbys < roles.StandBys {
		log.Printf("for jepsen: extra online spare")
		bad = true
	}
	if numOnlineStandbys > roles.StandBys && numOnlineVoters < roles.Voters {
		log.Printf("for jepsen: extra online standby")
		bad = true
	}

	// Check whether the number of failure domains with at least one live voter (standby) is maximized.
	byFailureDomain := map[uint64][]client.NodeInfo{}
	for node, meta := range changes.State {
		if meta != nil {
			byFailureDomain[meta.FailureDomain] = append(byFailureDomain[meta.FailureDomain], node)
		}
	}
	numDomainsWithStandby := 0
	numDomainsWithVoter := 0
	numSingletonVoters := 0
	for _, nodes := range byFailureDomain {
		for _, node := range nodes {
			if node.Role == client.StandBy {
				numDomainsWithStandby += 1
			}
			if node.Role == client.Voter {
				numDomainsWithVoter += 1
				if len(nodes) == 1 {
					numSingletonVoters += 1
				}
			}
		}
	}

	if numDomainsWithVoter < len(byFailureDomain) && numDomainsWithVoter < numOnlineVoters {
		log.Printf("for jepsen: online voters are not spread out")
		bad = true
	}
	// An online voter in a singleton domain excludes a standby
	// from occupying that same domain, so we may not be able to
	// get the number of domains with standbys as high as
	// desired.
	adjustedNumStandbyDomains := numDomainsWithStandby + numSingletonVoters
	if adjustedNumStandbyDomains < len(byFailureDomain) && adjustedNumStandbyDomains < numOnlineStandbys {
		log.Printf("for jepsen: online standbys are not spread out")
		bad = true
	}

	if bad {
		log.Printf("changes=%v offlines=%v byFailureDomain=%v", changes, offlines, byFailureDomain)
	}

	return "nil", nil
}

func fileExists(dir, file string) (bool, error) {
	path := filepath.Join(dir, file)

	if _, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return false, fmt.Errorf("check if %s exists: %w", file, err)
		}
		return false, nil
	}

	return true, nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	limit := syscall.Rlimit{Cur: 0xffffffffffffffff, Max: 0xffffffffffffffff}
	err := syscall.Setrlimit(syscall.RLIMIT_CORE, &limit)
	if err != nil {
		log.Fatalf("set core limit: %v", err)
	}

	removed := false
	rejoin := false

	dir := flag.String("dir", "", "data directory")
	node := flag.String("node", "", "node name")
	cluster := flag.String("cluster", "", "names of all nodes in the cluster")
	latency := flag.Int("latency", 5, "average one-way network latency, in msecs")

	flag.Parse()

	addr, err := net.ResolveIPAddr("ip", *node)
	if err != nil {
		log.Fatalf("resolve node address: %v", err)
	}

	removed, err = fileExists(*dir, "removed")
	if err != nil {
		log.Fatalf("check if 'removed' file exists: %v", err)
	}

	rejoin, err = fileExists(*dir, "rejoin")
	if err != nil {
		log.Fatalf("check if 'rejoin' file exists: %v", err)
	}

	log.Printf("starting %q with IP %q and cluster %q", *node, addr.IP.String(), *cluster)

	if removed {
		log.Printf("node was removed")
		return
	}

	nodes := strings.Split(*cluster, ",")
	options := []app.Option{
		app.WithAddress(makeAddress(addr.IP.String(), port+1)),
		app.WithLogFunc(dqliteLog),
		app.WithNetworkLatency(time.Duration(*latency) * time.Millisecond),
		app.WithRolesAdjustmentFrequency(time.Second),
		app.WithSnapshotParams(dqlite.SnapshotParams{Threshold: 128, Trailing: 1024}),
	}

	// When rejoining set app.WithCluster() to the full list of existing
	// nodes, otherwise set it only to the preceeding ones.
	if rejoin {
		options = append(options, app.WithCluster(otherAddresses(*node, nodes)))
	} else {
		options = append(options, app.WithCluster(preceedingAddresses(*node, nodes)))
	}

	// Spawn the dqlite server thread.
	a, err := app.New(*dir, options...)
	if err != nil {
		log.Fatalf("create app: %v", err)
	}

	// Wait for the cluster to be stable (possibly joining this node).
	if err := a.Ready(context.Background()); err != nil {
		log.Fatalf("wait app ready: %v", err)
	}

	log.Printf("app ready")

	// Open the app database.
	db, err := a.Open(context.Background(), "app")
	if err != nil {
		log.Fatalf("open database: %v", err)
	}

	// Ensure the SQL schema is there, possibly retrying upon contention.
	for i := 0; i < 10; i++ {
		_, err := db.Exec(schema)
		if err == nil {
			break
		}
		if i == 9 {
			log.Fatalf("create schema: database still locked after 10 retries", err)
		}
		if err, ok := err.(driver.Error); ok && err.Code == driver.ErrBusy {
			time.Sleep(250 * time.Millisecond)
			continue
		}
		log.Fatalf("create schema: %v", err)
	}

	// Request timeout. If latency is 2 milliseconds, election timeout will
	// be 30 milliseconds, so a request timeout of 200 milliseconds should
	// allow a few election rounds to triger.
	timeout := 100 * time.Duration(*latency) * time.Millisecond

	roles := app.RolesConfig{Voters: 3, StandBys: 3}

	// Handle API requests.
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		result := ""
		err := fmt.Errorf("bad request")

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		switch r.URL.Path {
		case "/append":
			switch r.Method {
			case "POST":
				value, _ := ioutil.ReadAll(r.Body)
				result, err = appendPost(ctx, db, string(value))
			}
		case "/bank":
			switch r.Method {
			case "GET":
				result, err = bankGet(ctx, db)
			case "PUT":
				value, _ := ioutil.ReadAll(r.Body)
				result, err = bankPut(ctx, db, string(value))
			case "POST":
				value, _ := ioutil.ReadAll(r.Body)
				result, err = bankPost(ctx, db, string(value))
			}
		case "/set":
			switch r.Method {
			case "GET":
				result, err = setGet(ctx, db)
			case "POST":
				value, _ := ioutil.ReadAll(r.Body)
				result, err = setPost(ctx, db, string(value))
			}
		case "/leader":
			switch r.Method {
			case "GET":
				result, err = leaderGet(ctx, a)
			}
		case "/members":
			switch r.Method {
			case "GET":
				result, err = membersGet(ctx, a)
			case "DELETE":
				value, _ := ioutil.ReadAll(r.Body)
				result, err = membersDelete(ctx, a, string(value))
			}
		case "/ready":
			switch r.Method {
			case "GET":
				result, err = readyGet(ctx, a, nodes)
			}
		case "/stable":
			switch r.Method {
			case "GET":
				result, err = stableGet(ctx, a, nodes, roles, false)
			}
		case "/health":
			switch r.Method {
			case "GET":
				result, err = stableGet(ctx, a, nodes, roles, true)
			}
		}
		if err != nil {
			result = fmt.Sprintf("Error: %s", err.Error())
		}
		fmt.Fprintf(w, "%s", result)
	})

	listener, err := net.Listen("tcp", makeAddress(addr.IP.String(), port))
	if err != nil {
		log.Fatalf("listen to API address: %v", err)
	}

	log.Printf("serve API requests")

	go http.Serve(listener, nil)

	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT)
	signal.Notify(ch, syscall.SIGTERM)

	<-ch

	log.Printf("received shutdown signal")

	ctxHandover, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	a.Handover(ctxHandover)

	db.Close()
	a.Close()

	log.Printf("exit")
}
