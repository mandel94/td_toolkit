
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
# And one level
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from scrape_content.scrape_archive import scrape_archive



if __name__ == "__main__":
    articles = scrape_archive("reports/monthly/archive_page.html")
    import pandas as pd
    df = pd.DataFrame(articles)
    print(df.shape)
    print(df.head())