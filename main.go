package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"flag"
	"log"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	htmltomarkdown "github.com/JohannesKaufmann/html-to-markdown/v2"
	"github.com/PuerkitoBio/goquery"
	"github.com/jxskiss/base62"
	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
	"github.com/yosssi/gohtml"
)

func main() {
	ctx := context.Background()

	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	var dbPath string
	var urlsRaw string
	fs.StringVar(&dbPath, "db", "data.db", "database file path")
	fs.StringVar(&urlsRaw, "urls", "", "comma-separated list of project URLs to scrape, otherwise all")
	fs.Parse(os.Args[1:])

	var urls []string
	if urlsRaw != "" {
		urls = strings.Split(urlsRaw, ",")
	}
	if err := run(ctx, dbPath, urls); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, dbPath string, onlyURLs []string) error {
	db, err := sql.Open("sqlite3", "file:"+dbPath+"?_pragma=foreign_keys(1)")
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS contents (id INTEGER PRIMARY KEY, hash TEXT UNIQUE, html TEXT, markdown TEXT)`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS projects (id INTEGER PRIMARY KEY, url TEXT UNIQUE, title TEXT, state TEXT)`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS project_observations (id INTEGER PRIMARY KEY, project_id INTEGER REFERENCES projects (id), t DATETIME, content_id INTEGER REFERENCES contents (id))`)
	if err != nil {
		return err
	}

	projectsURL, err := url.Parse("https://www.shapeyourcityhalifax.ca/projects")
	if err != nil {
		return err
	}
	abs := func(s string) string {
		rel, err := url.Parse(s)
		if err != nil {
			return ""
		}
		return projectsURL.ResolveReference(rel).String()
	}

	sels, err := get(ctx, "https://www.shapeyourcityhalifax.ca/projects", []string{".project-tile"})
	if err != nil {
		return err
	}

	type Project struct {
		Title string
		State string
		URL   string

		HTMLSum  string
		HTML     string
		Markdown string
	}
	var projects []Project
	for _, project := range sels[0].EachIter() {
		p := Project{
			State: project.AttrOr("data-state", ""),
			URL:   abs(project.Find("a.project-tile__link").AttrOr("href", "")),
		}
		if p.URL == "https://www.shapeyourcityhalifax.ca/shape-your-city-halifax" {
			continue
		}
		if len(onlyURLs) > 0 && !slices.Contains(onlyURLs, p.URL) {
			continue
		}
		projects = append(projects, p)
	}

	for i, p := range projects {
		log.Printf("fetching %v/%v %v", i+1, len(projects), p.URL)
		sels, err := get(ctx, p.URL, []string{"#yield"})
		if err != nil {
			return err
		}

		removes := []string{
			"#map-layers",
			"div[data-markers]",
			"input[name=authenticity_token]",
			"div.widget_follow_project",
			"div.widget_related_projects",
			"#qanda_description_text",
			"script",
			".SocialSharing",
			"[name=a_comment_body]",
		}
		for _, s := range removes {
			sels[0].Find(s).Remove()
		}

		for _, input := range sels[0].Find("input").EachIter() {
			input.RemoveAttr("id")
		}
		for _, label := range sels[0].Find("label").EachIter() {
			label.RemoveAttr("for")
		}
		for _, a := range sels[0].Find("a").EachIter() {
			a.SetAttr("href", abs(a.AttrOr("href", "")))
		}
		for _, img := range sels[0].Find("img").EachIter() {
			img.SetAttr("src", abs(img.AttrOr("src", "")))
		}

		p.Title = sels[0].Find("h1").First().Text()
		p.HTML, _ = sels[0].Html()
		p.HTML = gohtml.Format(p.HTML)
		p.Markdown, err = htmltomarkdown.ConvertString(p.HTML)
		if err != nil {
			return err
		}

		sum := sha256.Sum224([]byte(p.HTML))
		p.HTMLSum = base62.EncodeToString(sum[:])

		err = func() error {
			tx, err := db.Begin()
			if err != nil {
				return err
			}
			defer tx.Rollback()

			_, err = tx.Exec(`INSERT OR REPLACE INTO contents (id, hash, html, markdown) VALUES ((SELECT id FROM contents WHERE hash = ?), ?, ?, ?)`, p.HTMLSum, p.HTMLSum, p.HTML, p.Markdown)
			if err != nil {
				return err
			}
			_, err = tx.Exec(`INSERT OR REPLACE INTO projects (id, url, title, state) VALUES ((SELECT id FROM projects WHERE url = ?), ?, ?, ?)`, p.URL, p.URL, p.Title, p.State)
			if err != nil {
				return err
			}

			_, err = tx.Exec(`INSERT INTO project_observations (project_id, t, content_id) VALUES ((SELECT id FROM projects WHERE url = ?), ?, (SELECT id FROM contents WHERE hash = ?))`, p.URL, time.Now(), p.HTMLSum)
			if err != nil {
				return err
			}

			if err := tx.Commit(); err != nil {
				return err
			}
			return nil
		}()
		if err != nil {
			return err
		}

		time.Sleep(time.Second)
	}

	return nil
}

func get(ctx context.Context, u string, selectors []string) ([]*goquery.Selection, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, err
	}

	var sels []*goquery.Selection
	for _, sel := range selectors {
		sel := doc.Find(sel)
		if sel == nil {
			return nil, nil
		}
		sels = append(sels, sel)
	}
	return sels, nil
}
