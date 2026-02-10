package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	} else if d < time.Hour {
		minutes := int(d.Minutes())
		seconds := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	} else {
		hours := int(d.Hours())
		minutes := int(d.Minutes()) % 60
		return fmt.Sprintf("%dh %dm", hours, minutes)
	}
}

type TournamentDetails struct {
	EventCode             string   `json:"event_code,omitempty"`
	TournamentName        string   `json:"tournament_name,omitempty"`
	City                  string   `json:"city,omitempty"`
	Country               string   `json:"country,omitempty"`
	NumberOfPlayers       string   `json:"number_of_players,omitempty"`
	System                string   `json:"system,omitempty"`
	Hybrid                string   `json:"hybrid,omitempty"`
	Category              string   `json:"category,omitempty"`
	StartDate             string   `json:"start_date,omitempty"`
	EndDate               string   `json:"end_date,omitempty"`
	DateReceived          string   `json:"date_received,omitempty"`
	DateRegistered        string   `json:"date_registered,omitempty"`
	Type                  string   `json:"type,omitempty"`
	TimeControl           string   `json:"time_control,omitempty"`
	Zone                  string   `json:"zone,omitempty"`
	ReportedMultRoundDays string   `json:"reported_mult_round_days,omitempty"`
	ChiefArbiter          []string `json:"chief_arbiter,omitempty"`
	DeputyChiefArbiter    []string `json:"deputy_chief_arbiter,omitempty"`
	Arbiter               []string `json:"arbiter,omitempty"`
	AssistantArbiter      []string `json:"assistant_arbiter,omitempty"`
	ChiefOrganizer        []string `json:"chief_organizer,omitempty"`
	Organizer             []string `json:"organizer,omitempty"`
	NatChampionship       string   `json:"nat_championship,omitempty"`
	PGNFile               string   `json:"pgn_file,omitempty"`
	OrigReport            string   `json:"orig_report,omitempty"`
	ViewReportHref        string   `json:"view_report_href,omitempty"`
	ViewReportText        string   `json:"view_report_text,omitempty"`
}

type Result struct {
	TournamentID string             `json:"tournament_id"`
	Success      bool               `json:"success"`
	Details      *TournamentDetails `json:"details,omitempty"`
	Error        string             `json:"error,omitempty"`
}

type RateLimiter struct {
	tokens     float64
	maxTokens  float64
	refillRate float64
	lastRefill time.Time
	errorCount int
}

func NewRateLimiter(requestsPerSecond float64) *RateLimiter {
	return &RateLimiter{
		tokens:     requestsPerSecond,
		maxTokens:  requestsPerSecond * 2,
		refillRate: requestsPerSecond,
		lastRefill: time.Now(),
	}
}

func (rl *RateLimiter) Wait() {
	now := time.Now()
	elapsed := now.Sub(rl.lastRefill).Seconds()
	rl.tokens += elapsed * rl.refillRate
	if rl.tokens > rl.maxTokens {
		rl.tokens = rl.maxTokens
	}
	rl.lastRefill = now

	if rl.tokens < 1.0 {
		waitTime := time.Duration((1.0-rl.tokens)/rl.refillRate*1000) * time.Millisecond
		time.Sleep(waitTime)
		rl.tokens = 0
	} else {
		rl.tokens -= 1.0
	}
}

func (rl *RateLimiter) RecordSuccess() {
	if rl.errorCount == 0 && rl.refillRate < 2.0 {
		rl.refillRate *= 1.05
	}
	rl.errorCount = 0
}

func (rl *RateLimiter) RecordError() {
	rl.errorCount++
	if rl.errorCount > 2 {
		rl.refillRate *= 0.5
		if rl.refillRate < 0.2 {
			rl.refillRate = 0.2
		}
		log.Printf("Rate limited! Slowing down to %.2f req/s", rl.refillRate)
	}
}

func (rl *RateLimiter) GetRate() float64 {
	return rl.refillRate
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

func extractTextFromCell(cell *goquery.Selection) string {
	hasLinks := cell.Find("a").Length() > 0
	if !hasLinks {
		return strings.TrimSpace(cell.Text())
	}

	var parts []string
	cell.Find("a").Each(func(i int, s *goquery.Selection) {
		linkText := strings.TrimSpace(s.Text())
		if linkText != "" {
			parts = append(parts, linkText)
		}
	})

	cloned := cell.Clone()
	cloned.Find("a").Remove()
	remaining := strings.TrimSpace(cloned.Text())
	if remaining != "" {
		parts = append(parts, remaining)
	}

	if len(parts) == 0 {
		return strings.TrimSpace(cell.Text())
	}
	return strings.TrimSpace(strings.Join(parts, " "))
}

func extractLinksFromCell(cell *goquery.Selection) []string {
	var links []string
	cell.Find("a").Each(func(i int, s *goquery.Selection) {
		text := strings.TrimSpace(s.Text())
		if text != "" {
			links = append(links, text)
		}
	})
	return links
}

func extractLinkHref(cell *goquery.Selection) string {
	href, exists := cell.Find("a").First().Attr("href")
	if !exists {
		return ""
	}
	return href
}

func fetchTournamentDetails(tournamentID string, client *http.Client) (*TournamentDetails, error) {
	url := fmt.Sprintf("https://ratings.fide.com/tournament_information.phtml?event=%s", tournamentID)

	maxRetries := 3
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(100*(1<<uint(attempt-1))) * time.Millisecond
			time.Sleep(delay)
		}

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
		req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
		req.Header.Set("Accept-Language", "en-US,en;q=0.9")
		req.Header.Set("Connection", "close")
		req.Header.Set("Cache-Control", "max-age=0")

		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("network error: %w", err)
			if strings.Contains(err.Error(), "EOF") ||
				strings.Contains(err.Error(), "connection reset") {
				continue
			}
			return nil, lastErr
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
		}

		bodyBytes, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("read error: %w", err)
			continue
		}

		time.Sleep(10 * time.Millisecond)

		doc, err := goquery.NewDocumentFromReader(strings.NewReader(string(bodyBytes)))
		if err != nil {
			return nil, fmt.Errorf("parse error: %w", err)
		}

		details := &TournamentDetails{}

		if doc.Find("table.details_table").Length() == 0 {
			return nil, fmt.Errorf("no data found")
		}

		doc.Find("table.details_table tr").Each(func(i int, s *goquery.Selection) {
			labelCell := s.Find("td.info_table_l")
			valueCell := s.Find("td").Eq(1)

			if labelCell.Length() == 0 || valueCell.Length() == 0 {
				return
			}

			label := strings.TrimSpace(labelCell.Text())
			value := extractTextFromCell(valueCell)

			switch label {
			case "Event code":
				details.EventCode = value
			case "Tournament Name":
				details.TournamentName = value
			case "City":
				details.City = value
			case "Country":
				details.Country = value
			case "Number of players":
				details.NumberOfPlayers = value
			case "System":
				details.System = value
			case "Hybrid":
				details.Hybrid = value
			case "Category":
				details.Category = value
			case "Start Date":
				details.StartDate = value
			case "End Date":
				details.EndDate = value
			case "Date received":
				details.DateReceived = value
			case "Date registered":
				details.DateRegistered = value
			case "Type":
				details.Type = value
			case "Time Control":
				details.TimeControl = value
			case "Zone":
				details.Zone = value
			case "Reported mult. round days":
				details.ReportedMultRoundDays = value
			case "Nat. Championship":
				details.NatChampionship = value
			case "Chief Arbiter":
				details.ChiefArbiter = extractLinksFromCell(valueCell)
			case "Deputy Chief Arbiter":
				details.DeputyChiefArbiter = extractLinksFromCell(valueCell)
			case "Arbiter":
				details.Arbiter = extractLinksFromCell(valueCell)
			case "Assistant Arbiter":
				details.AssistantArbiter = extractLinksFromCell(valueCell)
			case "Chief Organizer":
				details.ChiefOrganizer = extractLinksFromCell(valueCell)
			case "Organizer":
				details.Organizer = extractLinksFromCell(valueCell)
			case "PGN file":
				details.PGNFile = value
			case "Orig.Report":
				details.OrigReport = extractLinkHref(valueCell)
			case "View Report":
				details.ViewReportHref = extractLinkHref(valueCell)
				details.ViewReportText = extractTextFromCell(valueCell)
			}
		})

		return details, nil
	}

	return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
}

func main() {
	var (
		inputFile  = flag.String("input", "", "Path to tournament IDs file")
		year       = flag.Int("year", 0, "Year to process")
		month      = flag.Int("month", 0, "Month to process")
		dataDir    = flag.String("data-dir", "data", "Base data directory")
		outputFile = flag.String("output", "", "Output JSON file")
		rateLimit  = flag.Float64("rate-limit", 1.5, "Initial requests per second (default: 1.5)")
		maxRetries = flag.Int("max-retries", 3, "Max retry passes (default: 3)")
		checkpoint = flag.Int("checkpoint", 100, "Save every N tournaments (default: 100)")
		showTime   = flag.Bool("show-time", false, "Show timing info")
	)
	flag.Parse()

	var inputPath string
	if *inputFile != "" {
		inputPath = *inputFile
	} else if *year > 0 && *month > 0 {
		if *month < 1 || *month > 12 {
			log.Fatal("Error: month must be 1-12")
		}
		inputPath = filepath.Join(*dataDir, "tournament_ids", fmt.Sprintf("%d_%02d", *year, *month))
	} else {
		log.Fatal("Error: specify --input or --year and --month")
	}

	var outputPath string
	if *outputFile != "" {
		outputPath = *outputFile
	} else if *year > 0 && *month > 0 {
		outputPath = filepath.Join(*dataDir, "tournament_details", fmt.Sprintf("%d_%02d.json", *year, *month))
	}

	tournamentIDs, err := readTournamentIDs(inputPath)
	if err != nil {
		log.Fatalf("Error reading IDs: %v", err)
	}

	if len(tournamentIDs) == 0 {
		log.Fatal("No tournament IDs found")
	}

	log.Printf("Processing %d tournaments", len(tournamentIDs))
	log.Printf("Settings: %.2f req/s initial rate, checkpoint every %d", *rateLimit, *checkpoint)

	startTime := time.Now()

	transport := &http.Transport{
		MaxIdleConns:          0,
		MaxIdleConnsPerHost:   0,
		IdleConnTimeout:       0,
		DisableCompression:    false,
		DisableKeepAlives:     true,
		ResponseHeaderTimeout: 30 * time.Second,
		ForceAttemptHTTP2:     false,
	}
	client := &http.Client{
		Timeout:   45 * time.Second,
		Transport: transport,
	}

	rateLimiter := NewRateLimiter(*rateLimit)

	var allResults []Result
	successCount := 0
	errorCount := 0

	saveCheckpoint := func() {
		if outputPath == "" || *checkpoint == 0 {
			return
		}

		checkpointPath := outputPath + ".checkpoint"
		file, err := os.Create(checkpointPath)
		if err != nil {
			log.Printf("Checkpoint save failed: %v", err)
			return
		}
		defer file.Close()

		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(allResults); err != nil {
			log.Printf("Checkpoint encode failed: %v", err)
		}
	}

	currentTournaments := tournamentIDs

	for pass := 0; pass <= *maxRetries; pass++ {
		if len(currentTournaments) == 0 {
			break
		}

		if pass > 0 {
			delay := time.Duration(3<<uint(pass-1)) * time.Second
			log.Printf("Retry pass %d: waiting %v before retrying %d tournaments",
				pass, formatDuration(delay), len(currentTournaments))
			time.Sleep(delay)
		}

		var passFailed []string

		for _, tournamentID := range currentTournaments {
			rateLimiter.Wait()

			details, err := fetchTournamentDetails(tournamentID, client)

			result := Result{TournamentID: tournamentID}

			if err != nil {
				errorCount++
				result.Success = false
				result.Error = err.Error()

				if strings.Contains(err.Error(), "EOF") ||
					strings.Contains(err.Error(), "connection reset") {
					rateLimiter.RecordError()
				}

				if strings.Contains(err.Error(), "EOF") ||
					strings.Contains(err.Error(), "timeout") ||
					strings.Contains(err.Error(), "connection reset") {
					if pass < *maxRetries {
						passFailed = append(passFailed, tournamentID)
					}
				}
			} else {
				successCount++
				result.Success = true
				result.Details = details
				rateLimiter.RecordSuccess()

				if *checkpoint > 0 && successCount%*checkpoint == 0 {
					log.Printf("Saving checkpoint at %d successful...", successCount)
					saveCheckpoint()
				}
			}

			allResults = append(allResults, result)

			totalProcessed := successCount + errorCount
			elapsed := time.Since(startTime)
			avgTime := elapsed / time.Duration(totalProcessed)
			remaining := len(tournamentIDs) - totalProcessed
			estRemaining := avgTime * time.Duration(remaining)

			if *showTime {
				rate := rateLimiter.GetRate()
				if result.Success {
					name := "unknown"
					if result.Details != nil && result.Details.TournamentName != "" {
						name = result.Details.TournamentName
					}
					log.Printf("[%d/%d] ✓ %s: %s | Rate: %.2f/s | Est: %v",
						totalProcessed, len(tournamentIDs), tournamentID, name,
						rate, formatDuration(estRemaining))
				} else {
					log.Printf("[%d/%d] ✗ %s: %s | Rate: %.2f/s",
						totalProcessed, len(tournamentIDs), tournamentID,
						result.Error, rate)
				}
			}

			if totalProcessed%50 == 0 || totalProcessed == len(tournamentIDs) {
				actualRate := float64(totalProcessed) / elapsed.Seconds()
				targetRate := rateLimiter.GetRate()
				log.Printf("Progress: %d/%d (%d✓ %d✗) | Actual: %.2f/s | Target: %.2f/s | Elapsed: %v | Est: %v",
					totalProcessed, len(tournamentIDs),
					successCount, errorCount,
					actualRate, targetRate,
					formatDuration(elapsed), formatDuration(estRemaining))
			}
		}

		currentTournaments = passFailed
	}

	// Save final results
	var output io.Writer
	if outputPath != "" {
		os.MkdirAll(filepath.Dir(outputPath), 0755)
		file, err := os.Create(outputPath)
		if err != nil {
			log.Fatalf("Failed to create output: %v", err)
		}
		defer file.Close()
		output = file
	} else {
		output = os.Stdout
	}

	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(allResults); err != nil {
		log.Fatalf("Failed to encode: %v", err)
	}

	totalTime := time.Since(startTime)
	finalRate := float64(successCount+errorCount) / totalTime.Seconds()

	log.Printf("\nFinal Summary:")
	log.Printf("  Total: %d", len(tournamentIDs))
	log.Printf("  Success: %d (%.1f%%)", successCount, 100.0*float64(successCount)/float64(len(tournamentIDs)))
	log.Printf("  Errors: %d", errorCount)
	log.Printf("  Time: %s", formatDuration(totalTime))
	log.Printf("  Average rate: %.2f tournaments/sec", finalRate)
	if outputPath != "" {
		log.Printf("  Output: %s", outputPath)
	}
}