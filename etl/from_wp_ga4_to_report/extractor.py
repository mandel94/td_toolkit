import pandas as pd
import xml.etree.ElementTree as ET
# import config  # Removed direct import of config

class Extractor:
    def __init__(self, config):
        """
        Initializes the Extractor with a configuration dictionary.
        Args:
            config (dict): A dictionary containing 'GA4_FILE_PATH', 
                           'WP_FILE_PATH', and 'WP_FILE_TYPE'.
        """
        self.ga4_file_path = config['GA4_FILE_PATH']
        self.wp_file_path = config['WP_FILE_PATH']
        self.wp_file_type = config.get('WP_FILE_TYPE', 'xml') # Default to xml if not provided

    def extract_ga4_data(self):
        """Extracts data from the GA4 CSV file."""
        try:
            # Skip the first 9 rows and use the 10th row as header
            ga4_df = pd.read_csv(self.ga4_file_path, skiprows=9, header=0)
            return ga4_df
        except Exception as e:
            print(f"Error extracting GA4 data: {e}")
            return pd.DataFrame()

    def extract_wp_data(self):
        """Extracts data from the WordPress export file (XML or CSV)."""
        if self.wp_file_type == "xml":
            return self._extract_wp_xml()
        elif self.wp_file_type == "csv":
            return self._extract_wp_csv()
        else:
            print(f"Unsupported WordPress file type: {self.wp_file_type}")
            return pd.DataFrame()

    def _extract_wp_xml(self):
        """Extracts data from a WordPress XML export file."""
        try:
            tree = ET.parse(self.wp_file_path)
            root = tree.getroot()
            
            posts_data = []
            
            # Namespace dictionary to handle XML namespaces
            ns = {
                'wp': 'http://wordpress.org/export/1.2/',
                'content': 'http://purl.org/rss/1.0/modules/content/',
                'dc': 'http://purl.org/dc/elements/1.1/'
            }

            for item in root.findall('.//channel/item'):
                post_type = item.find('wp:post_type', ns)
                if post_type is not None and post_type.text == 'post': # Ensure it's a blog post
                    title = item.find('title').text if item.find('title') is not None else None
                    link = item.find('link').text if item.find('link') is not None else None
                    pub_date_str = item.find('pubDate').text if item.find('pubDate') is not None else None
                    
                    # Extract categories
                    categories = []
                    for cat_element in item.findall('category[@domain="category"]'):
                        categories.append(cat_element.text)
                    category_str = ', '.join(categories) if categories else None

                    # Extract content
                    content_encoded = item.find('content:encoded', ns)
                    content = content_encoded.text if content_encoded is not None else None

                    # Extract Yoast SEO metadata
                    focus_kw = None
                    meta_desc = None
                    linkdex = None
                    
                    for postmeta in item.findall('wp:postmeta', ns):
                        meta_key_element = postmeta.find('wp:meta_key', ns)
                        meta_value_element = postmeta.find('wp:meta_value', ns)
                        if meta_key_element is not None and meta_value_element is not None:
                            meta_key = meta_key_element.text
                            meta_value = meta_value_element.text
                            if meta_key == '_yoast_wpseo_focuskw':
                                focus_kw = meta_value
                            elif meta_key == '_yoast_wpseo_metadesc':
                                meta_desc = meta_value
                            elif meta_key == '_yoast_wpseo_linkdex':
                                linkdex = meta_value
                                
                    posts_data.append({
                        'title': title,
                        'link': link,
                        'category': category_str,
                        'pubdate': pub_date_str,
                        '_yoast_wpseo_focuskw': focus_kw,
                        '_yoast_wpseo_metadesc': meta_desc,
                        '_yoast_wpseo_linkdex': linkdex,
                        'content': content
                    })
            
            return pd.DataFrame(posts_data)
        except ET.ParseError as e:
            print(f"Error parsing WordPress XML file: {e}")
            return pd.DataFrame()
        except Exception as e:
            print(f"An unexpected error occurred during WordPress XML extraction: {e}")
            return pd.DataFrame()

    def _extract_wp_csv(self):
        """Extracts data from a WordPress CSV export file."""
        try:
            wp_df = pd.read_csv(self.wp_file_path)
            return wp_df
        except Exception as e:
            print(f"Error extracting WordPress CSV data: {e}")
            return pd.DataFrame()

    def extract_all_data(self):
        """Extracts data from all configured sources."""
        ga4_data = self.extract_ga4_data()
        wp_data = self.extract_wp_data()
        return ga4_data, wp_data

# Example usage (for testing, will be removed or adapted in main etl.py)
if __name__ == '__main__':
    example_config = {
        "GA4_FILE_PATH": "../../data/Pagine_e_schermate_Percorso_pagina_e_classe_schermata_ultimo_anno_9marzo2025.csv",
        "WP_FILE_PATH": "../../data/taxidriversit.WordPress.2025-04-14.xml",
        "WP_FILE_TYPE": "xml"
    }
    extractor = Extractor(config=example_config)
    ga4_df, wp_df = extractor.extract_all_data()
    
    print("GA4 Data:")
    print(ga4_df.head())
    print(f"GA4 Data Shape: {ga4_df.shape}")
    
    print("\nWordPress Data:")
    print(wp_df.head())
    print(f"WP Data Shape: {wp_df.shape}")
    if not wp_df.empty:
        print(wp_df.columns)
        if '_yoast_wpseo_focuskw' in wp_df.columns:
            print("Yoast Focus Keyword sample:", wp_df['_yoast_wpseo_focuskw'].dropna().head())
        if 'content' in wp_df.columns:
            print("Content sample:", wp_df['content'].str.slice(0,50).dropna().head())
