import os
from datetime import datetime
import pandas as pd
from playwright.sync_api import sync_playwright


def fetch_spotrac_payroll():
    """
    Scrapes Blue Jays payroll data from Spotrac using Playwright
    and saves it as a CSV file. Returns the file path.
    """
    url = "https://www.spotrac.com/mlb/toronto-blue-jays/payroll/"
    filename = f"payroll_{datetime.now().strftime('%Y%m%d')}.csv"
    output_path = f"/opt/airflow/data/{filename}"

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with sync_playwright() as p:
        
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-dev-shm-usage"]  
        )

        user_agent = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
        context = browser.new_context(user_agent=user_agent)
        page = context.new_page()

        print(f"Connecting to {url}...")
        try:
            page.goto(url, timeout=90000)
            page.wait_for_selector("table.datatable", timeout=15000)
            data = page.evaluate('''() => {
                const rows = Array.from(
                    document.querySelectorAll('table.datatable tbody tr')
                );
                return rows.map(row => {
                    const cells = row.querySelectorAll('td');
                    if (cells.length > 2) {
                        return {
                            player: cells[0].innerText.trim(),
                            salary_text: cells[2].innerText.trim()
                        };
                    }
                    return null;
                }).filter(item => item !== null);
            }''')
        except Exception as e:
            print(f"Error during scraping: {e}")
            browser.close()
            raise e

        browser.close()

    df = pd.DataFrame(data)
    df['salary'] = df['salary_text'].replace(r'[\$,]', '', regex=True)
    df['salary'] = pd.to_numeric(df['salary'], errors='coerce').fillna(0)
    df.to_csv(output_path, index=False)
    print(f"Successfully saved {len(df)} records to {output_path}")
    return output_path


if __name__ == "__main__":
    fetch_spotrac_payroll()
