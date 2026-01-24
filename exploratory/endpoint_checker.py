from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    
    # Capture network requests
    requests_log = []
    page.on("request", lambda req: requests_log.append({
        "url": req.url,
        "method": req.method,
        "post_data": req.post_data,
        "headers": dict(req.headers)
    }))
    
    page.goto("https://ratings.fide.com/rated_tournaments.phtml?country=USA&period=2025-01-01")
    page.wait_for_timeout(5000)
    
    # Find the AJAX call
    for req in requests_log:
        if "tournament" in req["url"].lower() or "a_" in req["url"]:
            print(f"\n{'='*60}")
            print(f"URL: {req['url']}")
            print(f"Method: {req['method']}")
            print(f"Post Data: {req['post_data']}")
            print(f"Headers: {req['headers']}")
    
    browser.close()