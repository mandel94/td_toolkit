import os
import pickle
from numpy import full
import requests
from bs4 import BeautifulSoup
import pandas as pd
import multiprocessing

# Mock selectors (to be customized by user)
SELECTORS = {
    "title": '#mvp-post-head > h1',
    "date": "#mvp-post-head > div > div.mvp-author-info-text.left.relative > div.mvp-author-info-date.left.relative > span.mvp-post-date.updated > time",
    "content": "#mvp-post-content",
    # Add more selectors as needed
}


def scrape_article(url, selectors):
    """
    Scrape a single article page using the provided selectors.
    Returns a dictionary with the original url (pagepath) and extracted fields.
    For the 'content' key, returns the raw HTML of the selected element.
    """
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        article = {"pagepath": url}
        for key, selector in selectors.items():
            element = soup.select_one(selector)
            if key == "content" and element:
                article[key] = str(element)  # Get raw HTML for content
            else:
                article[key] = element.get_text(strip=True) if element else None
        print(f"Scraped {url} successfully.")
        print(f"Extracted data: {article}")
        return pd.DataFrame([article])
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return {"pagepath": url, "error": str(e)}
    

def feature_engineering(df):
    """
    Perform feature engineering on the DataFrame.
    This function is a placeholder and can be extended later.
    """
    print("Performing feature engineering...")
    # Example: Add a new column with the length of the content
    if "content" in df.columns:
        # Parse html code of content using beautifulsoup
        content = df["content"].apply(lambda x: BeautifulSoup(x, "html.parser").get_text() if isinstance(x, str) else "")
    df["content_length"] = content.str.len()
    df["number_of_images"] = content.apply(lambda x: len(BeautifulSoup(x, "html.parser").find_all("img")) if isinstance(x, str) else 0)
    df["number_of_links"] = content.apply(lambda x: len(BeautifulSoup(x, "html.parser").find_all("a")) if isinstance(x, str) else 0)
    df["number_of_words"] = content.apply(lambda x: len(x.split()) if isinstance(x, str) else 0)
    df["number_of_paragraphs"] = content.apply(lambda x: len(BeautifulSoup(x, "html.parser").find_all("p")) if isinstance(x, str) else 0)
    df["number_of_headings"] = content.apply(lambda x: len(BeautifulSoup(x, "html.parser").find_all(["h1", "h2", "h3", "h4", "h5", "h6"])) if isinstance(x, str) else 0)
    df["number_of_videos"] = content.apply(lambda x: len(BeautifulSoup(x, "html.parser").find_all("video")) if isinstance(x, str) else 0)
    df["number_of_audios"] = content.apply(lambda x: len(BeautifulSoup(x, "html.parser").find_all("audio")) if isinstance(x, str) else 0)
    df["number_of_lists"] = content.apply(lambda x: len(BeautifulSoup(x, "html.parser").find_all(["ul", "ol"])) if isinstance(x, str) else 0)
    df["number_of_quotes"] = content.apply(lambda x: len(BeautifulSoup(x, "html.parser").find_all("blockquote")) if isinstance(x, str) else 0)
    df["number_of_code_blocks"] = content.apply(lambda x: len(BeautifulSoup(x, "html.parser").find_all("pre")) if isinstance(x, str) else 0)

    print("Feature engineering completed.")
    return df


def filter_columns(df):
    """
    Filter the DataFrame to keep only the columns specified in SELECTORS.
    """
    columns_to_keep = [
        "Percorso pagina e classe schermata",
        "Visualizzazioni",
        "Utenti attivi",
        "Visualizzazioni per utente attivo",
        "Durata media del coinvolgimento per utente attivo",
        "Conteggio eventi",
    ]
    return (
        df[columns_to_keep] if all(col in df.columns for col in columns_to_keep) else df
    )


def process_path(args):
    path, domain, selectors = args
    full_url = domain + path if path.startswith("/") else domain + "/" + path
    print(f"Scraping {full_url}...")
    article = scrape_article(full_url, selectors)
    article = feature_engineering(article)
    return article


def main():
    domain = "https://taxidrivers.it"  # Replace with the actual domain
    output_dir = "scrape_content/output"
    input_csv = os.path.join("etl", "output", "dev_transformed_page_and_screen_data.csv")
    df = pd.read_csv(input_csv)
    pagepaths = df["Percorso pagina e classe schermata"].tolist()
    print(len(pagepaths), "pagepaths found in the input CSV.")
    # Multiprocessing for parallel scraping
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        args = [(path, domain, SELECTORS) for path in pagepaths]
        articles = pool.map(process_path, args)
    output_path = os.path.join(output_dir, "scraped_articles.csv")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    all_articles_df = pd.concat(articles, ignore_index=True)
    all_articles_df.to_csv(output_path, index=False)
    print(f"All articles scraped and saved to {output_path}")

if __name__ == "__main__":
    main()
