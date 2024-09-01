package main

import (
	"compress/gzip"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"net/http"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func askArchive(client *http.Client, page string) (*http.Response, io.Reader, error) {
	req, err := http.NewRequest("GET", page, nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Add("accept-encoding", "gzip")
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	var r io.Reader
	if resp.Header.Get("content-encoding") == "gzip" {
		r, err = gzip.NewReader(resp.Body)
		if err != nil {
			resp.Body.Close()
			return nil, nil, err
		}
	} else {
		r = resp.Body
	}
	return resp, r, nil
}

func askArchiveForJson(client *http.Client, page string, dst any) error {
	resp, reader, err := askArchive(client, page)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(reader)
	err = dec.Decode(&dst)
	resp.Body.Close()
	return err
}

type CollectionSubset struct {
	Resp struct {
		Count uint `json:"numFound"`
		Start uint `json:"start"`
		Buf   []struct {
			Name string `json:"identifier"`
		} `json:"docs"`
	} `json:"response"`
}

func NewCollectionSubset(client *http.Client, collectionName string, count int, page int) (*CollectionSubset, error) {
	if count < 1 || page < 1 {
		return nil, fmt.Errorf("count (%d) and page (%d) must be >= 1", count, page)
	}
	var co CollectionSubset
	err := askArchiveForJson(client, "https://archive.org/advancedsearch.php?q=collection:"+collectionName+"&fl[]=identifier&rows="+fmt.Sprint(count)+"&page="+fmt.Sprint(page)+"&sort=downloads+desc&output=json", &co)
	if err != nil {
		return nil, err
	}
	return &co, nil
}

type ItemMetadata struct {
	Files []struct {
		Hash string `json:"sha1"`
		Name string `json:"name"`
	} `json:"result"`
	IsCollection bool
}

func NewItemMetadata(client *http.Client, item string) (*ItemMetadata, error) {
	var im ItemMetadata
	var t struct {
		Mediatype string `json:"result"`
	}
	err := askArchiveForJson(client, "https://archive.org/metadata/"+item+"/metadata/mediatype", &t)
	if err != nil {
		return nil, err
	}
	im.IsCollection = t.Mediatype == "collection";
	if im.IsCollection {
		return &im, nil
	}
	err = askArchiveForJson(client, "https://archive.org/metadata/"+item+"/files", &im)
	if err != nil {
		return nil, err
	}
	return &im, nil
}

type Storage struct {
	db      *sql.DB
	insName *sql.Stmt
	insHash *sql.Stmt
}

func NewStorage(dbPath string) (*Storage, error) {
	var s Storage
	var err error

	s.db, err = sql.Open("sqlite3", "hashes.db")
	if err != nil {
		log.Fatal(err)
	}

	_, err = s.db.Exec(`CREATE TABLE IF NOT EXISTS archive_items (
id INTEGER PRIMARY KEY AUTOINCREMENT,
name VARCHAR(255) UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS hashes (
hash BINARY(20) PRIMARY KEY,
item INTEGER,
FOREIGN KEY (item) REFERENCES archive_item(id)
);`)
	if err != nil {
		return nil, err
	}

	s.insName, err = s.db.Prepare(`INSERT INTO archive_items (name) VALUES (?);`)
	if err != nil {
		s.Close()
		return nil, err
	}
	s.insHash, err = s.db.Prepare(`INSERT INTO hashes (hash, item) VALUES (?, ?);`)
	if err != nil {
		s.Close()
		return nil, err
	}

	return &s, nil
}

func (s *Storage) Close() {
	if s.insHash != nil {
		s.insHash.Close()
	}
	if s.insName != nil {
		s.insName.Close()
	}
	if s.db != nil {
		s.db.Close()
	}
}

func (s *Storage) NewEntry(im *ItemMetadata, item string) (err error) {
	if len(im.Files) == 0 {
		return fmt.Errorf("no files")
	}

	tx, err := s.db.Begin()
	if err != nil {
		return
	}

	res, err := s.insName.Exec(item)
	if err != nil {
		tx.Rollback()
		return
	}
	id, err := res.LastInsertId()
	if err != nil {
		tx.Rollback()
		return
	}

	inserted := false
	for _, f := range im.Files {
		if f.Name == "__ia_thumb.jpg" {
			continue
		}
		if strings.HasPrefix(f.Name, item) {
			suffix := f.Name[len(item):]
			if suffix == "_archive.torrent" || suffix == "_files.xml" || suffix == "_meta.sqlite" || suffix == "_meta.xml" || suffix == "_reviews.xml" {
				continue
			}
		}

		if len(f.Hash) != 40 {
			log.Printf("item %s: file %s: hash '%s' would not be 20 bytes\n", item, f.Name, f.Hash)
			continue
		}
		var hexed []byte
		hexed, err = hex.DecodeString(f.Hash)
		if err != nil {
			log.Printf("item %s: %v in %s\n", item, err, f.Hash)
			err = nil
			continue
		}
		res, err = s.insHash.Exec(hexed, id)
		if err != nil {
			log.Printf("item %s: file %s: %v\n", item, f.Name, err)
			err = nil
			continue
			//tx.Rollback()
			//return
		}
		inserted = true
	}
	if !inserted {
		tx.Rollback()
		return fmt.Errorf("no valid files")
	}

	tx.Commit()
	return
}

type Job struct {
	collection string
	page       int
}

type Tasks struct {
	db        *sql.DB
	next      *sql.Stmt
	increment *sql.Stmt
	add       *sql.Stmt
	remove    *sql.Stmt
	remember  *sql.Stmt
	hasDone   *sql.Stmt
	length    int
}

func NewTasks(dbPath string) (*Tasks, error) {
	// yes, this code is ugly. no, I don't know a better way

	var t Tasks
	var err error

	t.db, err = sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	_, err = t.db.Exec(`CREATE TABLE IF NOT EXISTS jobs (
name VARCHAR(255) PRIMARY KEY,
page INTEGER
);
CREATE INDEX IF NOT EXISTS idx_page ON jobs(page);
CREATE TABLE IF NOT EXISTS done (
name VARCHAR(255) PRIMARY KEY,
page INTEGER,
reason TEXT
)`)
	if err != nil {
		t.Close()
		return nil, err
	}

	err = t.db.QueryRow(`SELECT COUNT(*) FROM jobs;`).Scan(&t.length)
	if err != nil {
		t.Close()
		return nil, err
	}

	t.next, err = t.db.Prepare(`SELECT name, page FROM jobs ORDER BY page ASC LIMIT 1;`)
	if err != nil {
		t.Close()
		return nil, err
	}
	t.increment, err = t.db.Prepare(`UPDATE jobs SET page = page + 1 WHERE name = (?);`)
	if err != nil {
		t.Close()
		return nil, err
	}
	t.add, err = t.db.Prepare(`INSERT INTO jobs (name, page) VALUES (?, ?) ON CONFLICT DO NOTHING;`)
	if err != nil {
		t.Close()
		return nil, err
	}
	t.remove, err = t.db.Prepare(`DELETE FROM jobs WHERE name = (?);`)
	if err != nil {
		t.Close()
		return nil, err
	}
	t.remember, err = t.db.Prepare(`INSERT INTO done (name, page, reason) VALUES (?, ?, ?);`)
	if err != nil {
		t.Close()
		return nil, err
	}
	t.hasDone, err = t.db.Prepare(`SELECT 1 FROM done WHERE name = (?);`)
	if err != nil {
		t.Close()
		return nil, err
	}

	return &t, nil
}

func (t *Tasks) Len() int { return t.length }

func (t *Tasks) Next() *Job {
	var job Job
	err := t.next.QueryRow().Scan(&job.collection, &job.page)
	if err != nil {
		log.Fatal(err)
	}
	return &job
}

func (t *Tasks) Increment(name string) {
	_, err := t.increment.Exec(name)
	if err != nil {
		log.Fatal(err)
	}
}

func (t *Tasks) Add(name string) {
	var done int
	err := t.hasDone.QueryRow().Scan(&done)
	if err == nil && done == 1 {
		return
	}
	_, err = t.add.Exec(name, int(1))
	if err != nil {
		log.Fatal(err)
	}
	t.length++
}

func (t *Tasks) Remove(job *Job, reason string) {
	_, err := t.remove.Exec(job.collection)
	if err != nil {
		log.Fatal(err)
	}
	_, err = t.remember.Exec(job.collection, job.page, reason)
	if err != nil {
		log.Printf("failed to remember deletion of %v %v by reason %v: %v\n", job.collection, job.page, reason, err)
	}
	t.length--
}

func (t *Tasks) Close() {
	if t.next != nil {
		t.next.Close()
	}
	if t.increment != nil {
		t.increment.Close()
	}
	if t.add != nil {
		t.add.Close()
	}
	if t.remove != nil {
		t.remove.Close()
	}
	if t.remember != nil {
		t.remember.Close()
	}
	if t.hasDone != nil {
		t.hasDone.Close()
	}
	if t.db != nil {
		t.db.Close()
	}
}

const batchSize = 1000

func main() {
	storage, err := NewStorage("hashes.db")
	if err != nil {
		log.Fatal(err)
	}
	defer storage.Close()

	tasks, err := NewTasks("working.db")
	if err != nil {
		log.Fatal(err)
	}
	defer tasks.Close()
	for i := 1; i < len(os.Args); i++ {
		tasks.Add(os.Args[i])
	}

	var client http.Client

	intr := make(chan os.Signal, 1)
	signal.Notify(intr, os.Interrupt)

	for tasks.Len() > 0 {
		select {
		case <-intr:
			log.Println("interrupted; shut down safely")
			return
		default:
			break
		}

		job := tasks.Next()

		co, err := NewCollectionSubset(&client, job.collection, batchSize, job.page)
		if err != nil {
			job.page++
			co, err = NewCollectionSubset(&client, job.collection, batchSize, job.page)
			if err != nil {
				tasks.Remove(job, fmt.Sprint(err))
				log.Printf("removed %v due to error %v\n", job.collection, err)
				continue
			}
			tasks.Increment(job.collection)
		}

		if len(co.Resp.Buf) == 0 /*|| job.page > foo*/ {
			tasks.Remove(job, "")
			continue
		}
		for _, itm := range co.Resp.Buf {
			time.Sleep(1000 * time.Millisecond)
			im, err := NewItemMetadata(&client, itm.Name)
			if err != nil {
				log.Println(err)
				continue
			}
			if im.IsCollection {
				tasks.Add(itm.Name)
				continue
			}
			err = storage.NewEntry(im, itm.Name)
			if err != nil {
				log.Printf("in item %s: %v\n", itm.Name, err)
			}
		}
		tasks.Increment(job.collection)
	}
}
