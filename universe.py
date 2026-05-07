"""
Univers de tickers suivis pour la collecte automatique de données fondamentales.
"""

SP500_TOP100 = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "BRK-B", "LLY", "AVGO",
    "JPM", "V", "UNH", "XOM", "MA", "COST", "PG", "JNJ", "HD", "ABBV",
    "MRK", "NFLX", "BAC", "CRM", "CVX", "AMD", "PEP", "KO", "ORCL", "TMO",
    "ADBE", "ACN", "WMT", "MCD", "ABT", "CSCO", "LIN", "TXN", "NEE", "PM",
    "RTX", "DHR", "ISRG", "VZ", "AMGN", "IBM", "CAT", "GE", "INTU", "QCOM",
    "SPGI", "UNP", "HON", "BKNG", "GS", "MS", "NOW", "BLK", "AXP", "TJX",
    "SYK", "ETN", "SBUX", "PLD", "GILD", "VRTX", "LOW", "MMC", "ADI", "LRCX",
    "DE", "ADP", "CB", "BSX", "REGN", "MDT", "MU", "C", "PANW", "ZTS",
    "SO", "CI", "SCHW", "DUK", "WM", "EOG", "SNPS", "KLAC", "CME", "ICE",
    "MO", "SLB", "MDLZ", "ITW", "HCA", "USB", "PNC", "NOC", "GD", "FI",
]

CAC40 = [
    "AI.PA", "AIR.PA", "ALO.PA", "MT.AS", "ATO.PA", "CS.PA", "BNP.PA", "EN.PA",
    "CAP.PA", "CA.PA", "ACA.PA", "BN.PA", "DSY.PA", "ENGI.PA", "EL.PA", "ERF.PA",
    "RMS.PA", "KER.PA", "OR.PA", "MC.PA", "ML.PA", "ORA.PA", "RI.PA", "PUB.PA",
    "RNO.PA", "SAF.PA", "SGO.PA", "SAN.PA", "SU.PA", "GLE.PA", "STLAP.PA",
    "STM.PA", "TEP.PA", "HO.PA", "FP.PA", "URW.AS", "VIE.PA", "DG.PA", "WLN.PA",
    "VIV.PA",
]

DAX_TOP30 = [
    "ADS.DE", "AIR.DE", "ALV.DE", "BAS.DE", "BAYN.DE", "BMW.DE", "BNR.DE",
    "CON.DE", "1COV.DE", "DB1.DE", "DBK.DE", "DHL.DE", "DTE.DE", "EOAN.DE",
    "FME.DE", "FRE.DE", "HEI.DE", "HEN3.DE", "IFX.DE", "MRK.DE", "MTX.DE",
    "MUV2.DE", "P911.DE", "PAH3.DE", "QIA.DE", "RHM.DE", "RWE.DE", "SAP.DE",
    "SIE.DE", "VOW3.DE",
]

UNIVERSE_ALL = sorted(set(SP500_TOP100 + CAC40 + DAX_TOP30))
