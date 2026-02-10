package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run test_tournament_scrape.go <tournament_id>")
	}

	tournamentID := os.Args[1]
	url := fmt.Sprintf("https://ratings.fide.com/tournament_information.phtml?event=%s", tournamentID)

	fmt.Printf("Fetching tournament %s from: %s\n\n", tournamentID, url)

	// Create request with proper headers
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatalf("Failed to create request: %v", err)
	}

	// Set realistic headers
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("Failed to fetch: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Fatalf("HTTP %d", resp.StatusCode)
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		log.Fatalf("Failed to parse HTML: %v", err)
	}

	fmt.Println("=== HTML Structure Debug ===")
	
	// Find the details table
	tableCount := doc.Find("table.details_table").Length()
	fmt.Printf("Found %d table(s) with class 'details_table'\n", tableCount)

	rowCount := doc.Find("table.details_table tr").Length()
	fmt.Printf("Found %d row(s) in details_table\n\n", rowCount)

	fmt.Println("=== Parsed Tournament Details ===")
	
	details := make(map[string]interface{})

	doc.Find("table.details_table tr").Each(func(i int, s *goquery.Selection) {
		labelCell := s.Find("td.info_table_l")
		valueCell := s.Find("td").Eq(1) // Second td is the value

		if labelCell.Length() == 0 || valueCell.Length() == 0 {
			return
		}

		label := strings.TrimSpace(labelCell.Text())
		rawHTML, _ := valueCell.Html()
		textValue := strings.TrimSpace(valueCell.Text())

		// Extract text from links
		var linkTexts []string
		valueCell.Find("a").Each(func(i int, a *goquery.Selection) {
			linkText := strings.TrimSpace(a.Text())
			if linkText != "" {
				linkTexts = append(linkTexts, linkText)
			}
		})

		// Store the value
		if len(linkTexts) > 0 {
			details[label] = linkTexts
		} else {
			details[label] = textValue
		}

		// Debug output
		fmt.Printf("\nRow %d:\n", i+1)
		fmt.Printf("  Label: '%s'\n", label)
		fmt.Printf("  Raw HTML: %s\n", rawHTML)
		fmt.Printf("  Text value: '%s'\n", textValue)
		if len(linkTexts) > 0 {
			fmt.Printf("  Link texts: %v\n", linkTexts)
		}
		fmt.Printf("  Stored value: %v\n", details[label])
	})

	fmt.Println("\n=== Final Parsed Data (JSON) ===")
	jsonData, err := json.MarshalIndent(details, "", "  ")
	if err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)
	}
	fmt.Println(string(jsonData))

	// Check for tournament name specifically
	if name, ok := details["Tournament Name"]; ok {
		fmt.Printf("\n=== Tournament Name: '%v' (type: %T) ===\n", name, name)
	} else {
		fmt.Println("\n=== WARNING: 'Tournament Name' field not found! ===")
	}
}

