from abc import ABC, abstractmethod
import os
import sys
import pandas as pd
from sklearn.preprocessing import StandardScaler
from kmodes.kprototypes import KPrototypes

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from gemini.gemini.gemini_client import GeminiKeywordCategoryMapper


# Ensure the gemini module is in the path

class SeoReport(ABC):
    """
    Abstract base class for SEO reports.
    """
    @abstractmethod
    def load_data(self, input_filename):
        """
        Load data from a CSV file.
        :param input_filename: Path to the input CSV file.
        :return: DataFrame containing the loaded data.
        """
        pass

    def cluster_keywords(self, df, n_clusters=4, random_state=42, api_key=None, model="gemini-2.5-flash"):
        """
        Esegue una cluster analysis su 'categoria', 'sottocategoria', 'Pos', 'Volume' usando KPrototypes per dati misti.
        Prima mappa le keyword a categoria e sottocategoria tramite Gemini.
        Aggiunge una colonna 'cluster' al DataFrame.
        """
        if api_key is None:
            raise ValueError("api_key (Gemini) richiesto per la mappatura delle categorie.")
        # Mappa le keyword a categoria e sottocategoria
        df = self.map_keywords_to_categories(df, api_key=api_key, model=model)
        # Prepara i dati: 'categoria', 'sottocategoria' (categorici), 'Pos' e 'Volume' (numerici)
        data = df[['categoria', 'sottocategoria', 'Pos', 'Volume']].copy()
        data['Pos'] = pd.to_numeric(data['Pos'], errors='coerce').fillna(data['Pos'].median())
        data['Volume'] = pd.to_numeric(data['Volume'], errors='coerce')
        volume_median = data['Volume'].median()
        if pd.isna(volume_median):
            data['Volume'] = data['Volume'].fillna(0)
        else:
            data['Volume'] = data['Volume'].fillna(volume_median)
        pos_median = data['Pos'].median()
        if pd.isna(pos_median):
            pos_median = 0
        data['Pos'] = data['Pos'].fillna(pos_median)
        # KPrototypes richiede numpy array
        X = data.values
        # Indici delle colonne categoriche
        categorical = [0, 1]
        kproto = KPrototypes(n_clusters=n_clusters, random_state=random_state)
        clusters = kproto.fit_predict(X, categorical=categorical)
        df = df.reset_index(drop=True)
        df['cluster'] = clusters
        return df

    def map_keywords_to_categories(self, df, api_key, keyword_col="Keyword", category_col="categoria", subcategory_col="sottocategoria", model="gemini-2.5-flash"):
        """
        Usa GeminiKeywordCategoryMapper per mappare ogni keyword a una categoria e sottocategoria.
        Aggiunge due colonne al DataFrame: category_col e subcategory_col.
        """
        mapper = GeminiKeywordCategoryMapper(api_key=api_key, model=model)
        categories = []
        subcategories = []
        for kw in df[keyword_col]:
            result = mapper.map_keyword(str(kw))
            categories.append(result.get("categoria"))
            subcategories.append(result.get("sottocategoria"))
        df[category_col] = categories
        df[subcategory_col] = subcategories
        return df

class SeoZoomReport(SeoReport):
    """
    A class to handle SEO reports specifically for SeoZoom.
    Inherits from the base SeoReport class.
    """
    def __init__(self):
        """
        Initialize the SeoZoomReport class.
        """
        super().__init__()

    def load_data(self, input_filename):
        """
        Load data from a CSV file.
        :param input_filename: Path to the input CSV file.
        :return: DataFrame containing the loaded data.
        """
        df = pd.read_csv(input_filename)
        return df

if __name__ == "__main__":
    report = SeoZoomReport()
    API_KEY = os.getenv("GEMINI_API_KEY")
    data = report.load_data("data/seo/Arte e intrattenimento_NicheKeywords.csv")
    enriched_data = report.map_keywords_to_categories(data, api_key=API_KEY, model="gemini-2.5-flash")
    print(enriched_data[['Keyword', 'categoria', 'sottocategoria']])
    # clustered = report.cluster_keywords(data, n_clusters=4)
    # print(clustered[['Keyword', 'Pos', 'Volume', 'cluster']].head())