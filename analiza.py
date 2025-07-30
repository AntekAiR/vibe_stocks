import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

def pobierz_dane(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Pobiera historyczne dane giełdowe dla danego tickera z Yahoo Finance.

    Args:
        ticker (str): Symbol giełdowy spółki (np. 'AAPL' dla Apple).
        start (str): Data początkowa w formacie 'YYYY-MM-DD'.
        end (str): Data końcowa w formacie 'YYYY-MM-DD'.

    Returns:
        pd.DataFrame: Ramka danych (DataFrame) z historycznymi danymi, 
                      lub None jeśli wystąpił błąd.
    """
    try:
        dane = yf.download(ticker, start=start, end=end)
        if dane.empty:
            print(f"Nie znaleziono danych dla tickera {ticker} w podanym zakresie dat.")
            return None
        print(f"Pobrano dane dla {ticker} od {start} do {end}.")
        return dane
    except Exception as e:
        print(f"Wystąpił błąd podczas pobierania danych: {e}")
        return None

def analizuj_i_rysuj_wykres(dane: pd.DataFrame, ticker: str):
    """
    Oblicza średnie kroczące i rysuje wykres ceny zamknięcia.

    Args:
        dane (pd.DataFrame): Ramka danych z danymi historycznymi.
        ticker (str): Symbol giełdowy spółki.
    """
    if dane is None or dane.empty:
        print("Brak danych do analizy.")
        return

    # Obliczanie średnich kroczących
    dane['SMA50'] = dane['Close'].rolling(window=50).mean()
    dane['SMA200'] = dane['Close'].rolling(window=200).mean()

    # Rysowanie wykresu
    plt.style.use('seaborn-v0_8-darkgrid')
    plt.figure(figsize=(14, 7))
    plt.plot(dane['Close'], label='Cena zamknięcia (Close)', color='blue', alpha=0.7)
    plt.plot(dane['SMA50'], label='Średnia krocząca 50-dniowa (SMA50)', color='orange', linestyle='--')
    plt.plot(dane['SMA200'], label='Średnia krocząca 200-dniowa (SMA200)', color='red', linestyle='--')

    plt.title(f'Analiza ceny akcji {ticker}')
    plt.xlabel('Data')
    plt.ylabel('Cena (USD)')
    plt.legend()
    plt.show()

if __name__ == "__main__":
    # --- Konfiguracja ---
    ticker_spolki = 'AAPL'  # Zmień na symbol innej spółki, np. 'MSFT', 'GOOGL', 'TSLA'
    data_poczatkowa = '2020-01-01'
    data_koncowa = datetime.now().strftime('%Y-%m-%d') # Dzisiejsza data

    dane_gieldowe = pobierz_dane(ticker_spolki, data_poczatkowa, data_koncowa)
    analizuj_i_rysuj_wykres(dane_gieldowe, ticker_spolki)