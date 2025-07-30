import pandas as pd
import yfinance as yf

def pobierz_liste_spolek_nasdaq() -> list:
    """
    Pobiera listę tickerów z serwera FTP NASDAQ i filtruje ją.

    Returns:
        list: Lista tickerów spółek (bez ETFów i instrumentów testowych).
    """
    # Adres URL do pliku z listą wszystkich symboli na NASDAQ
    url = "ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqtraded.txt"
    
    print("Pobieranie listy spółek z serwera NASDAQ...")
    try:
        # Używamy pandas do wczytania danych, separatorem jest '|'
        # Ostatnia linijka pliku to stopka, którą pomijamy (skipfooter=1)
        df = pd.read_csv(url, sep='|', skipfooter=1, engine='python')
        
        # Filtrujemy dane, aby zostawić tylko akcje zwykłe
        # Usuwamy ETFy ('N' w kolumnie ETF)
        # Usuwamy instrumenty testowe ('N' w kolumnie Test Issue)
        spolki = df[(df['Test Issue'] == 'N') & (df['ETF'] == 'N')]
        
        # Zwracamy listę symboli (tickerów)
        tickers = spolki['Symbol'].tolist()
        print(f"Pobrano i przefiltrowano listę. Znaleziono {len(tickers)} spółek.")
        return tickers

    except Exception as e:
        print(f"Nie udało się pobrać listy spółek. Błąd: {e}")
        return []

def znajdz_spolki_wzrostowe(tickers: list, limit_spolek: int = 4000):
    """
    Analizuje spółki i znajduje te, które spełniają kryteria wzrostu:
    - Wzrost 15-80% w ostatnim kwartale.
    - Wzrost 25-100% w ostatnim półroczu.
    - Kapitalizacja rynkowa > 100 mln USD.
    
    Args:
        tickers (list): Lista tickerów do analizy.
        limit_spolek (int): Maksymalna liczba spółek do przeanalizowania.
    """
    if not tickers:
        print("Lista tickerów jest pusta.")
        return

    tickers_do_analizy = tickers[:limit_spolek]
    print(f"Rozpoczynam analizę {len(tickers_do_analizy)} spółek...")

    # Pobieramy dane z ostatnich 7 miesięcy, aby mieć pewność, że pokryjemy 6 miesięcy handlowych.
    # auto_adjust=True automatycznie dostosowuje ceny o dywidendy i splity, co jest lepsze do analizy zwrotów.
    data = yf.download(tickers_do_analizy, period="7mo", auto_adjust=True, progress=True, group_by='ticker')

    if data.empty:
        print("Nie udało się pobrać danych dla analizy wzrostu.")
        return

    # Definicja okresów w dniach handlowych (przybliżenie)
    dni_kwartal = 63
    dni_polrocze = 126

    kandydaci = []

    print("\nKrok 1: Filtrowanie spółek według kryteriów wzrostu...")
    for ticker in tickers_do_analizy:
        try:
            historia_spolki = data[ticker]['Close'].dropna()

            # Sprawdzamy, czy mamy wystarczająco dużo danych historycznych
            if len(historia_spolki) < dni_polrocze + 1:
                continue  # Pomijamy spółkę, jeśli nie ma historii na 6 miesięcy

            # Pobieramy ceny: aktualną, sprzed kwartału i sprzed półrocza
            cena_aktualna = historia_spolki.iloc[-1]
            cena_kwartal_temu = historia_spolki.iloc[-dni_kwartal]
            cena_pol_roku_temu = historia_spolki.iloc[-dni_polrocze]

            # Obliczamy procentowe zmiany
            wzrost_kwartalny = (cena_aktualna / cena_kwartal_temu - 1) * 100
            wzrost_polroczny = (cena_aktualna / cena_pol_roku_temu - 1) * 100

            # Sprawdzamy, czy oba warunki są spełnione
            if (15 <= wzrost_kwartalny <= 80) and (25 <= wzrost_polroczny <= 100):
                kandydaci.append({
                    'Ticker': ticker,
                    'wzrost_kwartalny': wzrost_kwartalny,
                    'wzrost_polroczny': wzrost_polroczny
                })
        except (KeyError, IndexError, ZeroDivisionError):
            # Ignorujemy błędy dla pojedynczych spółek (brak danych, za krótka historia, cena=0)
            continue

    if not kandydaci:
        print("\nNie znaleziono żadnych spółek spełniających podane kryteria.")
        return

    print(f"\nZnaleziono {len(kandydaci)} kandydatów. Krok 2: Sprawdzanie kapitalizacji rynkowej (> 100M USD)...")
    
    znalezione_spolki = []
    for kandydat in kandydaci:
        try:
            ticker_obj = yf.Ticker(kandydat['Ticker'])
            market_cap = ticker_obj.info.get('marketCap')

            # Sprawdzamy warunek kapitalizacji
            if market_cap and market_cap > 100_000_000:
                # Formatowanie kapitalizacji dla czytelności (M dla milionów, B dla miliardów)
                if market_cap >= 1_000_000_000:
                    kapitalizacja_str = f"{market_cap / 1_000_000_000:.2f}B"
                else:
                    kapitalizacja_str = f"{market_cap / 1_000_000:.2f}M"

                znalezione_spolki.append({
                    'Ticker': kandydat['Ticker'],
                    'Wzrost (kwartał)': f"{kandydat['wzrost_kwartalny']:.2f}%",
                    'Wzrost (pół roku)': f"{kandydat['wzrost_polroczny']:.2f}%",
                    'Kapitalizacja': kapitalizacja_str
                })
        except Exception:
            # Ignorujemy błędy przy pobieraniu info dla pojedynczego tickera
            continue

    if not znalezione_spolki:
        print("\nNie znaleziono żadnych spółek spełniających wszystkie kryteria (wzrost + kapitalizacja).")
    else:
        print("\n--- Spółki spełniające wszystkie kryteria ---")
        wyniki_df = pd.DataFrame(znalezione_spolki)
        print(wyniki_df.to_string(index=False))

if __name__ == "__main__":
    lista_spolek = pobierz_liste_spolek_nasdaq()
    if lista_spolek:
        znajdz_spolki_wzrostowe(lista_spolek, limit_spolek=4000)