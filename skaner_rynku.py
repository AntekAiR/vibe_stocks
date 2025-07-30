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
    - Wzrost w ciągu ostatnich 3 dni mniejszy niż wzrost S&P 500.
    - Brak spadku kursu w ciągu ostatnich 6 dni roboczych.
    
    Args:
        tickers (list): Lista tickerów do analizy.
        limit_spolek (int): Maksymalna liczba spółek do przeanalizowania.
    """
    if not tickers:
        print("Lista tickerów jest pusta.")
        return

    tickers_do_analizy = tickers[:limit_spolek]
    print(f"Rozpoczynam analizę {len(tickers_do_analizy)} spółek...")

    # Krok 0: Pobierz dane dla S&P 500 PRZED głównym zapytaniem, aby uniknąć błędów w yfinance
    print("\nPobieranie danych dla S&P 500 (^GSPC)...")
    sp500_data = yf.download('^GSPC', period='5d', auto_adjust=True, progress=False)
    sp500_wzrost_3d = None
    if len(sp500_data) >= 4:
        # Odporne na błędy pobieranie kolumny 'Close', niezależnie od formatu zwracanego przez yfinance
        if isinstance(sp500_data.columns, pd.MultiIndex):
            # Jeśli kolumny są wielopoziomowe, 'Close' może być na górnym poziomie.
            # Wybieramy go, a następnie bierzemy pierwszą kolumnę danych.
            close_prices = sp500_data['Close'].iloc[:, 0]
        else:
            close_prices = sp500_data['Close']

        sp500_cena_teraz = close_prices.iloc[-1]
        sp500_cena_3d_temu = close_prices.iloc[-4]
        
        if sp500_cena_3d_temu > 0:
            sp500_wzrost_3d = (sp500_cena_teraz / sp500_cena_3d_temu - 1) * 100
            print(f"3-dniowy wzrost S&P 500: {sp500_wzrost_3d:.2f}%")
    else:
        print("Ostrzeżenie: Nie udało się pobrać wystarczających danych dla S&P 500, aby obliczyć 3-dniowy wzrost. Ten krok zostanie pominięty.")

    # Pobieramy dane z ostatnich 7 miesięcy, aby mieć pewność, że pokryjemy 6 miesięcy handlowych.
    # auto_adjust=True automatycznie dostosowuje ceny o dywidendy i splity, co jest lepsze do analizy zwrotów.
    print("Pobieranie danych historycznych dla spółek...")
    data = yf.download(tickers_do_analizy, period="7mo", auto_adjust=True, progress=True, group_by='ticker')

    if data.empty:
        print("Nie udało się pobrać danych dla analizy wzrostu.")
        return

    # Definicja okresów w dniach handlowych (przybliżenie)
    dni_kwartal = 63
    dni_polrocze = 126

    kandydaci = []

    print("\nKrok 1: Filtrowanie spółek według kryteriów wzrostu (kwartał/półrocze)...")
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
        print("\nNie znaleziono żadnych spółek spełniających kryteria wzrostu kwartalnego/półrocznego.")
        return

    print(f"\nZnaleziono {len(kandydaci)} kandydatów. Krok 2: Sprawdzanie kapitalizacji rynkowej (> 100M USD)...")
    
    kandydaci_po_kapitalizacji = []
    for kandydat in kandydaci:
        try:
            ticker_obj = yf.Ticker(kandydat['Ticker'])
            market_cap = ticker_obj.info.get('marketCap')

            # Sprawdzamy warunek kapitalizacji
            if market_cap and market_cap > 100_000_000:
                kandydat['marketCap'] = market_cap # Dodajemy do słownika do późniejszego formatowania
                kandydaci_po_kapitalizacji.append(kandydat)
        except Exception:
            # Ignorujemy błędy przy pobieraniu info dla pojedynczego tickera
            continue
    
    if not kandydaci_po_kapitalizacji:
        print("\nŻaden z kandydatów nie spełnił kryterium kapitalizacji rynkowej.")
        return

    if sp500_wzrost_3d is None:
        print("\nPominięto Krok 3 (porównanie z S&P 500) z powodu braku danych.")
        # W tym przypadku po prostu wyświetlamy wyniki z kroku 2
        finalne_spolki_dane = []
        for kandydat in kandydaci_po_kapitalizacji:
            market_cap = kandydat['marketCap']
            if market_cap >= 1_000_000_000:
                kapitalizacja_str = f"{market_cap / 1_000_000_000:.2f}B"
            else:
                kapitalizacja_str = f"{market_cap / 1_000_000:.2f}M"
            finalne_spolki_dane.append({
                'Ticker': kandydat['Ticker'],
                'Wzrost (kwartał)': f"{kandydat['wzrost_kwartalny']:.2f}%",
                'Wzrost (pół roku)': f"{kandydat['wzrost_polroczny']:.2f}%",
                'Kapitalizacja': kapitalizacja_str
            })
        wyniki_df = pd.DataFrame(finalne_spolki_dane)
        print("\n--- Spółki spełniające kryteria wzrostu i kapitalizacji ---")
        print(wyniki_df.to_string(index=False))
        return

    print(f"\nKrok 3: Filtrowanie {len(kandydaci_po_kapitalizacji)} spółek na podstawie 3-dniowego wzrostu (wolniej niż S&P 500)...")
    finalne_spolki = []
    for kandydat in kandydaci_po_kapitalizacji:
        try:
            # Używamy już pobranych danych
            historia_spolki = data[kandydat['Ticker']]['Close'].dropna()

            # Sprawdzamy, czy mamy wystarczająco danych do obliczenia 3-dniowego wzrostu
            if len(historia_spolki) >= 4:
                cena_teraz = historia_spolki.iloc[-1]
                cena_3d_temu = historia_spolki.iloc[-4]

                # Unikamy dzielenia przez zero
                if cena_3d_temu > 0:
                    wzrost_3d_spolki = (cena_teraz / cena_3d_temu - 1) * 100

                    # Porównujemy wzrost spółki ze wzrostem S&P 500
                    if wzrost_3d_spolki < sp500_wzrost_3d:
                        kandydat['wzrost_3d'] = wzrost_3d_spolki
                        finalne_spolki.append(kandydat)
        except (KeyError, IndexError, ZeroDivisionError):
            continue

    if not finalne_spolki:
        print("\nNie znaleziono żadnych spółek spełniających pierwsze trzy kryteria (wzrost, kap., porównanie z S&P500).")
        return

    # Krok 4: Eliminacja spółek, które spadły w ciągu ostatnich 6 dni.
    print(f"\nKrok 4: Filtrowanie {len(finalne_spolki)} spółek pod kątem braku spadku w ostatnich 6 dniach roboczych...")
    ostateczne_spolki = []
    for kandydat in finalne_spolki:
        try:
            historia_spolki = data[kandydat['Ticker']]['Close'].dropna()

            # Potrzebujemy co najmniej 7 dni danych (dzisiaj + 6 dni wstecz)
            if len(historia_spolki) >= 7:
                cena_teraz = historia_spolki.iloc[-1]
                cena_6d_temu = historia_spolki.iloc[-7]

                # Sprawdzamy, czy cena nie spadła
                if cena_teraz >= cena_6d_temu:
                    ostateczne_spolki.append(kandydat)
        except (KeyError, IndexError):
            continue

    if not ostateczne_spolki:
        print("\nNie znaleziono żadnych spółek spełniających wszystkie cztery kryteria.")
    else:
        # Przygotowanie danych do wyświetlenia
        finalne_spolki_dane = []
        for spolka in ostateczne_spolki:
            market_cap = spolka['marketCap']
            kapitalizacja_str = f"{market_cap / 1_000_000_000:.2f}B" if market_cap >= 1_000_000_000 else f"{market_cap / 1_000_000:.2f}M"
            finalne_spolki_dane.append({
                'Ticker': spolka['Ticker'], 'Wzrost (3D)': f"{spolka['wzrost_3d']:.2f}%",
                'Wzrost (kwartał)': f"{spolka['wzrost_kwartalny']:.2f}%", 'Wzrost (pół roku)': f"{spolka['wzrost_polroczny']:.2f}%",
                'Kapitalizacja': kapitalizacja_str
            })
        print("\n--- Spółki spełniające wszystkie kryteria ---")
        wyniki_df = pd.DataFrame(finalne_spolki_dane)
        wyniki_df = wyniki_df[['Ticker', 'Wzrost (3D)', 'Wzrost (kwartał)', 'Wzrost (pół roku)', 'Kapitalizacja']]
        print(wyniki_df.to_string(index=False))

        # Zapisywanie kluczowych wyników do pliku tekstowego
        nazwa_pliku = 'wynik.txt'
        try:
            with open(nazwa_pliku, 'w') as f:
                for spolka in ostateczne_spolki:
                    # Format: Ticker, wzrost 3 mies., wzrost 6 mies., wzrost 3 dni
                    linia = (
                        f"{spolka['Ticker']}, "
                        f"{spolka['wzrost_kwartalny']:.2f}, "
                        f"{spolka['wzrost_polroczny']:.2f}, "
                        f"{spolka['wzrost_3d']:.2f}\n"
                    )
                    f.write(linia)
                
                # Ostatnia linia z wynikiem S&P 500 dla spójności
                if sp500_wzrost_3d is not None:
                    f.write(f"SP500, {sp500_wzrost_3d:.2f}\n")
            print(f"\nKluczowe dane zostały zapisane do pliku: {nazwa_pliku}")
        except IOError as e:
            print(f"\nNie udało się zapisać wyników do pliku. Błąd: {e}")

    # Podsumowanie S&P 500 na koniec
    if not sp500_data.empty and len(sp500_data) > 1:
        zmiany_dzienne = sp500_data['Close'].diff().dropna()
        dni_wzrostu = (zmiany_dzienne > 0).sum()
        dni_spadku = (zmiany_dzienne < 0).sum()
        print(f"\nPodsumowanie S&P 500 z ostatnich {len(zmiany_dzienne)} dni handlowych: Dni wzrostu: {dni_wzrostu}, Dni spadku: {dni_spadku}.")

if __name__ == "__main__":
    lista_spolek = pobierz_liste_spolek_nasdaq()
    if lista_spolek:
        znajdz_spolki_wzrostowe(lista_spolek, limit_spolek=300)