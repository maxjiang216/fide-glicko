package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
)

// FieldInfo tracks information about a field
type FieldInfo struct {
	Count        int      `json:"count"`
	SampleValues []string `json:"sample_values"`
	HasLinks     bool     `json:"has_links"`
}

func main() {
	var (
		inputFile   = flag.String("input", "", "Path to file containing tournament IDs (one per line)")
		maxCheck    = flag.Int("max", 100, "Maximum number of tournaments to check (0 = all)")
		concurrency = flag.Int("concurrency", 5, "Maximum number of concurrent requests")
	)
	flag.Parse()

	if *inputFile == "" {
		log.Fatal("Error: --input flag is required")
	}

	// Read tournament IDs
	tournamentIDs, err := readTournamentIDs(*inputFile)
	if err != nil {
		log.Fatalf("Error reading tournament IDs: %v", err)
	}

	if len(tournamentIDs) == 0 {
		log.Fatal("Error: no tournament IDs found in input file")
	}

	// Limit the number of tournaments to check
	if *maxCheck > 0 && *maxCheck < len(tournamentIDs) {
		tournamentIDs = tournamentIDs[:*maxCheck]
	}

	log.Printf("Checking %d tournaments for all available fields...", len(tournamentIDs))

	// Map to store all fields found
	fieldsMap := make(map[string]*FieldInfo)
	var fieldsMutex sync.Mutex

	// Create HTTP client
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Semaphore for concurrency control
	semaphore := make(chan struct{}, *concurrency)
	var wg sync.WaitGroup

	// Process tournaments
	for i, tournamentID := range tournamentIDs {
		wg.Add(1)
		semaphore <- struct{}{}

		go func(id string, index int) {
			defer func() { <-semaphore }()
			defer wg.Done()

			fields, err := fetchTournamentFields(id, client)
			if err != nil {
				log.Printf("[%d/%d] Error fetching tournament %s: %v", index+1, len(tournamentIDs), id, err)
				return
			}

			// Update fields map
			fieldsMutex.Lock()
			for fieldName, fieldValue := range fields {
				if info, exists := fieldsMap[fieldName]; exists {
					info.Count++
					// Add sample value if we don't have many yet
					if len(info.SampleValues) < 5 && !contains(info.SampleValues, fieldValue) {
						info.SampleValues = append(info.SampleValues, fieldValue)
					}
					// Update has_links if this one has links
					if strings.Contains(fieldValue, "<a") {
						info.HasLinks = true
					}
				} else {
					fieldsMap[fieldName] = &FieldInfo{
						Count:        1,
						SampleValues: []string{fieldValue},
						HasLinks:     strings.Contains(fieldValue, "<a"),
					}
				}
			}
			fieldsMutex.Unlock()

			if (index+1)%10 == 0 {
				log.Printf("Processed %d/%d tournaments...", index+1, len(tournamentIDs))
			}
		}(tournamentID, i)

		// Small delay to avoid overwhelming the server
		time.Sleep(200 * time.Millisecond)
	}

	wg.Wait()

	// Print results
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("FIELDS FOUND IN TOURNAMENT PAGES")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("\nTotal tournaments checked: %d\n", len(tournamentIDs))
	fmt.Printf("Total unique fields found: %d\n\n", len(fieldsMap))

	// Sort fields by count (most common first)
	type fieldEntry struct {
		Name  string
		Info  *FieldInfo
		Count int
	}

	var sortedFields []fieldEntry
	for name, info := range fieldsMap {
		sortedFields = append(sortedFields, fieldEntry{
			Name:  name,
			Info:  info,
			Count: info.Count,
		})
	}

	// Simple sort by count (descending)
	for i := 0; i < len(sortedFields)-1; i++ {
		for j := i + 1; j < len(sortedFields); j++ {
			if sortedFields[i].Count < sortedFields[j].Count {
				sortedFields[i], sortedFields[j] = sortedFields[j], sortedFields[i]
			}
		}
	}

	// Print fields
	for _, entry := range sortedFields {
		fmt.Printf("Field: %-35s | Count: %4d/%d", entry.Name, entry.Info.Count, len(tournamentIDs))
		if entry.Info.HasLinks {
			fmt.Print(" | Has Links: YES")
		}
		fmt.Println()
		if len(entry.Info.SampleValues) > 0 {
			fmt.Printf("  Sample values:\n")
			for _, val := range entry.Info.SampleValues {
				// Truncate long values
				displayVal := val
				if len(displayVal) > 80 {
					displayVal = displayVal[:77] + "..."
				}
				// Remove HTML tags for display
				displayVal = strings.ReplaceAll(displayVal, "<b>", "")
				displayVal = strings.ReplaceAll(displayVal, "</b>", "")
				displayVal = strings.ReplaceAll(displayVal, "<strong>", "")
				displayVal = strings.ReplaceAll(displayVal, "</strong>", "")
				displayVal = strings.ReplaceAll(displayVal, "<a ", "[LINK]")
				displayVal = strings.ReplaceAll(displayVal, "</a>", "")
				fmt.Printf("    - %s\n", displayVal)
			}
		}
		fmt.Println()
	}

	// Save to JSON file
	outputData := make(map[string]interface{})
	outputData["total_tournaments_checked"] = len(tournamentIDs)
	outputData["total_unique_fields"] = len(fieldsMap)
	outputData["fields"] = fieldsMap

	outputFile := strings.TrimSuffix(*inputFile, filepath.Base(*inputFile)) + "tournament_fields.json"
	if strings.HasSuffix(*inputFile, ".txt") {
		outputFile = strings.TrimSuffix(*inputFile, ".txt") + "_fields.json"
	} else {
		outputFile = *inputFile + "_fields.json"
	}

	jsonData, err := json.MarshalIndent(outputData, "", "  ")
	if err != nil {
		log.Printf("Error marshaling JSON: %v", err)
	} else {
		if err := os.WriteFile(outputFile, jsonData, 0644); err != nil {
			log.Printf("Error writing JSON file: %v", err)
		} else {
			fmt.Printf("Results saved to: %s\n", outputFile)
		}
	}
}

func readTournamentIDs(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	var ids []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		id := strings.TrimSpace(scanner.Text())
		if id != "" {
			ids = append(ids, id)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return ids, nil
}

func fetchTournamentFields(tournamentID string, client *http.Client) (map[string]string, error) {
	url := fmt.Sprintf("https://ratings.fide.com/tournament_information.phtml?event=%s", tournamentID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse HTML: %w", err)
	}

	fields := make(map[string]string)

	// Find all rows with info_table_l class (the label cells)
	doc.Find("table.details_table tr").Each(func(i int, s *goquery.Selection) {
		labelCell := s.Find("td.info_table_l")
		valueCell := s.Find("td").Eq(1) // Second td is the value

		if labelCell.Length() == 0 || valueCell.Length() == 0 {
			return
		}

		label := strings.TrimSpace(labelCell.Text())
		if label == "" {
			return
		}

		// Get raw HTML of value cell for analysis
		htmlValue, _ := valueCell.Html()
		fields[label] = htmlValue
	})

	return fields, nil
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
