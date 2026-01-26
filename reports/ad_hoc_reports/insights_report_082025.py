

if __name__ == "__main__":
    df = pd.read_excel(DATA_PATH)
    print(df.head().columns)
    print(df.shape)