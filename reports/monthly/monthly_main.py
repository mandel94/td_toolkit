import pandas as pd
import os
import sys
from dotenv import load_dotenv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))




if __name__ == "__main__":
    data = pd.read_excel("output/monthly/monthly_top_articles.xlsx", sheet_name="Luglio")
    print(data.head())