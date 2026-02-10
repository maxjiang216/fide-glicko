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
	"sync"
	"sync/atomic"
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

// AdaptiveRateLimiter with token bucket algorithm
type AdaptiveRateLimiter struct {
	mu                sync.Mutex
	tokens            float64
	maxTokens         float64
	refillRate        float64 // tokens per second
	lastRefill        time.Time
	consecutiveErrors int32
	targetDelay       time.Duration
}

func NewAdaptiveRateLimiter(requestsPerSecond float64) *AdaptiveRateLimiter {
	return &AdaptiveRateLimiter{
		tokens:      requestsPerSecond,
		maxTokens:   requestsPerSecond * 2, // Allow bursting
		refillRate:  requestsPerSecond,
		lastRefill:  time.Now(),
		targetDelay: time.Duration(1000/requestsPerSecond) * time.Millisecond,
	}
}

func (rl *AdaptiveRateLimiter) Wait() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Refill tokens based on time elapsed
	now := time.Now()
	elapsed := now.Sub(rl.lastRefill).Seconds()
	rl.tokens += elapsed * rl.refillRate
	if rl.tokens > rl.maxTokens {
		rl.tokens = rl.maxTokens
	}
	rl.lastRefill = now

	// Wait if no tokens available
	if rl.tokens < 1.0 {
		waitTime := time.Duration((1.0-rl.tokens)/rl.refillRate*1000) * time.Millisecond
		rl.mu.Unlock()
		time.Sleep(waitTime)
		rl.mu.Lock()
		rl.tokens = 0
	} else {
		rl.tokens -= 1.0
	}
}

func (rl *AdaptiveRateLimiter) RecordSuccess() {
	old := atomic.SwapInt32(&rl.consecutiveErrors, 0)

	// Speed up if we've had sustained success
	if old == 0 {
		rl.mu.Lock()
		// Gradually increase rate (up to 2x original)
		if rl.refillRate < 2.0 {
			rl.refillRate *= 1.05
		}
		rl.mu.Unlock()
	}
}

func (rl *AdaptiveRateLimiter) RecordError() {
	count := atomic.AddInt32(&rl.consecutiveErrors, 1)

	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Exponential backoff on errors
	if count > 2 {
		rl.refillRate *= 0.5
		if rl.refillRate < 0.2 { // Min 1 request per 5 seconds
			rl.refillRate = 0.2
		}
		log.Printf("Rate limited! Slowing down to %.2f req/s", rl.refillRate)
	}
}

func (rl *AdaptiveRateLimiter) GetRate() float64 {
	rl.mu.Lock()
	defer rl.mu.Unlock()
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

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Cache-Control", "max-age=0")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("network error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)
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

func worker(id int, jobs <-chan string, results chan<- Result, client *http.Client,
	rateLimiter *AdaptiveRateLimiter, wg *sync.WaitGroup) {
	defer wg.Done()

	for tournamentID := range jobs {
		rateLimiter.Wait()

		details, err := fetchTournamentDetails(tournamentID, client)

		if err != nil {
			// Check if it's a rate limit error
			if strings.Contains(err.Error(), "EOF") ||
				strings.Contains(err.Error(), "connection reset") {
				rateLimiter.RecordError()
			}

			results <- Result{
				TournamentID: tournamentID,
				Success:      false,
				Error:        err.Error(),
			}
		} else {
			rateLimiter.RecordSuccess()
			results <- Result{
				TournamentID: tournamentID,
				Success:      true,
				Details:      details,
			}
		}
	}
}

func main() {
	var (
		inputFile   = flag.String("input", "", "Path to tournament IDs file")
		year        = flag.Int("year", 0, "Year to process")
		month       = flag.Int("month", 0, "Month to process")
		dataDir     = flag.String("data-dir", "data", "Base data directory")
		outputFile  = flag.String("output", "", "Output JSON file")
		concurrency = flag.Int("concurrency", 3, "Concurrent workers (default: 3)")
		rateLimit   = flag.Float64("rate-limit", 1.5, "Initial requests per second (default: 1.5)")
		maxRetries  = flag.Int("max-retries", 3, "Max retry passes (default: 3)")
		checkpoint  = flag.Int("checkpoint", 100, "Save every N tournaments (default: 100)")
		showTime    = flag.Bool("show-time", false, "Show timing info")
	)
	flag.Parse()

	// Determine paths
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
	log.Printf("Settings: %d workers, %.2f req/s initial rate, checkpoint every %d",
		*concurrency, *rateLimit, *checkpoint)

	startTime := time.Now()

	// HTTP client with connection reuse disabled (server closes connections after each request)
	transport := &http.Transport{
		MaxIdleConns:          *concurrency * 3,
		MaxIdleConnsPerHost:   *concurrency * 2,
		IdleConnTimeout:       90 * time.Second,
		DisableCompression:    false,
		DisableKeepAlives:     true, // Disable connection reuse to avoid EOF errors
		ResponseHeaderTimeout: 30 * time.Second,
	}
	client := &http.Client{
		Timeout:   45 * time.Second,
		Transport: transport,
	}

	rateLimiter := NewAdaptiveRateLimiter(*rateLimit)

	var (
		allResults   []Result
		resultsMutex sync.Mutex
		successCount int32
		errorCount   int32
	)

	saveCheckpoint := func() {
		if outputPath == "" || *checkpoint == 0 {
			return
		}
		resultsMutex.Lock()
		defer resultsMutex.Unlock()

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

		jobs := make(chan string, *concurrency*3)
		results := make(chan Result, *concurrency*3)
		var wg sync.WaitGroup

		// Start workers
		for w := 1; w <= *concurrency; w++ {
			wg.Add(1)
			go worker(w, jobs, results, client, rateLimiter, &wg)
		}

		// Feed jobs
		go func() {
			for _, id := range currentTournaments {
				jobs <- id
			}
			close(jobs)
		}()

		// Close results when done
		go func() {
			wg.Wait()
			close(results)
		}()

		// Collect results
		passFailed := make([]string, 0)
		lastCheckpoint := int32(0)

		for result := range results {
			resultsMutex.Lock()
			allResults = append(allResults, result)
			resultsMutex.Unlock()

			var totalProcessed int32
			if result.Success {
				totalProcessed = atomic.AddInt32(&successCount, 1) + atomic.LoadInt32(&errorCount)

				// Checkpoint
				if *checkpoint > 0 &&
					int(atomic.LoadInt32(&successCount)) > int(lastCheckpoint)+*checkpoint {
					lastCheckpoint = atomic.LoadInt32(&successCount)
					log.Printf("Saving checkpoint at %d successful...", lastCheckpoint)
					saveCheckpoint()
				}
			} else {
				totalProcessed = atomic.LoadInt32(&successCount) + atomic.AddInt32(&errorCount, 1)

				// Retry on network errors
				if strings.Contains(result.Error, "EOF") ||
					strings.Contains(result.Error, "timeout") ||
					strings.Contains(result.Error, "connection reset") {
					if pass < *maxRetries {
						passFailed = append(passFailed, result.TournamentID)
					}
				}
			}

			elapsed := time.Since(startTime)
			avgTime := elapsed / time.Duration(totalProcessed)
			remaining := len(tournamentIDs) - int(totalProcessed)
			estRemaining := avgTime * time.Duration(remaining)

			if *showTime && result.Success {
				rate := rateLimiter.GetRate()
				name := "unknown"
				if result.Details != nil && result.Details.TournamentName != "" {
					name = result.Details.TournamentName
				}
				log.Printf("[%d/%d] ✓ %s: %s | Rate: %.2f/s | Est: %v",
					totalProcessed, len(tournamentIDs), result.TournamentID, name,
					rate, formatDuration(estRemaining))
			} else if *showTime && !result.Success {
				rate := rateLimiter.GetRate()
				log.Printf("[%d/%d] ✗ %s: %s | Rate: %.2f/s",
					totalProcessed, len(tournamentIDs), result.TournamentID,
					result.Error, rate)
			}

			if int(totalProcessed)%50 == 0 || int(totalProcessed) == len(tournamentIDs) {
				actualRate := float64(totalProcessed) / elapsed.Seconds()
				targetRate := rateLimiter.GetRate()
				log.Printf("Progress: %d/%d (%d✓ %d✗) | Actual: %.2f/s | Target: %.2f/s | Elapsed: %v | Est: %v",
					totalProcessed, len(tournamentIDs),
					atomic.LoadInt32(&successCount), atomic.LoadInt32(&errorCount),
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
	finalRate := float64(atomic.LoadInt32(&successCount)+atomic.LoadInt32(&errorCount)) / totalTime.Seconds()

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
