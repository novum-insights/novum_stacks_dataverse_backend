import math
import numpy as np


def percent_change(df):
    price = df["price_usd"]
    pc = [(price[i + 1] / price[i] - 1) * 100 for i in range(0, len(price) - 1)]
    pc.insert(0, None)

    df['%_change'] = pc

    return df


def calculate_rsi(df):
    price = df["price_usd"]
    change = [price[i + 1] - price[i] for i in range(0, len(price) - 1)]
    change.insert(0, None)

    rsi_up = [change[i] if change[i] > 0 else 0 for i in range(1, len(change))]
    rsi_down = [abs(change[i]) if change[i] < 0 else 0 for i in range(1, len(change))]

    rsi_up.insert(0, None)
    rsi_down.insert(0, None)

    rsi_avg_up = [sum(rsi_up[1:8]) / len(rsi_up[1:8])]
    for i in range(0, len(rsi_up[8:])):
        rsi_avg_up.append((rsi_avg_up[-1] * 6 + rsi_up[8 + i]) / 7)
    for i in range(0, len(df) - len(rsi_avg_up)):
        rsi_avg_up.insert(0, None)

    rsi_avg_down = [sum(rsi_down[1:8]) / len(rsi_down[1:8])]
    for i in range(0, len(rsi_down[8:])):
        rsi_avg_down.append((rsi_avg_down[-1] * 6 + rsi_down[8 + i]) / 7)
    for i in range(0, len(df) - len(rsi_avg_down)):
        rsi_avg_down.insert(0, None)

    rsi = [(100 * rsi_avg_up[i]) / (rsi_avg_up[i] + rsi_avg_down[i]) if rsi_avg_up[i] != None else None for i in
           range(0, len(rsi_avg_up))]

    df['rsi'] = rsi
    df['rsi'] = df['rsi'].round(3)

    return df


def calculate_rvi(df):
    price = df['price_usd']
    temp = [math.log(price[i + 1] / price[i]) for i in range(0, len(price) - 1)]
    # temp = [round(x, 4) for x in temp]
    temp.insert(0, None)

    standard_deviation = []
    for i in range(1, len(temp) - 6):
        standard_deviation.append(np.std(np.array(temp[i:i + 7])))

    # standard_deviation = [round(x, 4) for x in standard_deviation]
    for i in range(0, len(df) - len(standard_deviation)):
        standard_deviation.insert(0, 0)

    df['standard_deviation'] = standard_deviation

    # price = df['price_usd']
    rvi_up = [standard_deviation[i] if price[i + 1] > price[i] else 0 for i in range(6, len(price) - 1)]
    rvi_down = [standard_deviation[i] if price[i + 1] < price[i] else 0 for i in range(6, len(price) - 1)]

    for i in range(0, len(df) - len(rvi_up)):
        rvi_up.insert(0, None)
        rvi_down.insert(0, None)

    rvi_avg_up = [sum(rvi_up[7:14]) / len(rvi_up[7:14])]
    for i in range(0, len(rvi_up[14:])):
        rvi_avg_up.append((rvi_avg_up[-1] * 6 + rvi_up[14 + i]) / 7)
    for i in range(0, len(df) - len(rvi_avg_up)):
        rvi_avg_up.insert(0, None)

    rvi_avg_down = [sum(rvi_down[7:14]) / len(rvi_down[7:14])]
    for i in range(0, len(rvi_down[14:])):
        rvi_avg_down.append((rvi_avg_down[-1] * 6 + rvi_down[14 + i]) / 7)
    for i in range(0, len(df) - len(rvi_avg_down)):
        rvi_avg_down.insert(0, None)

    rvi = [(100 * rvi_avg_up[i]) / (rvi_avg_up[i] + rvi_avg_down[i]) if rvi_avg_up[i] != None else None for i in
           range(0, len(rvi_avg_up))]

    df['rvi'] = rvi
    df['rvi'] = df['rvi'].round(3)
    return df


def calculate_change(df):
    rsi = df['rsi']
    rvi = df['rvi']

    rsi_change = [rsi[i + 1] - rsi[i] if rsi[i] != None else 0 for i in range(7, len(rsi) - 1)]
    rvi_change = [rvi[i + 1] - rvi[i] if rvi[i] != None else 0 for i in range(13, len(rvi) - 1)]

    rsi_change.insert(0, rsi[7])
    rvi_change.insert(0, rvi[13])

    for i in range(0, len(df) - len(rsi_change)):
        rsi_change.insert(0, 0)
    for i in range(0, len(df) - len(rvi_change)):
        rvi_change.insert(0, 0)

    df['rsi_change'] = rsi_change
    df['rvi_change'] = rvi_change

    return df


def calculate_ema(df):
    df['7dayEMA'] = df['price_usd'].ewm(span=7, adjust=False).mean()
    df['7dayEMA'] = df['7dayEMA'].round(3)
    return df


def calculate_nvt(df):
    market_cap = df["mktcap_usd"]
    volume = df["vol_usd"]
    nvt = market_cap / volume
    df['nvt'] = nvt
    df['nvt'] = df['nvt'].round(3)

    nvt_classic = [sum(nvt[i:i + 28]) / len(nvt[i:i + 28]) for i in range(0, len(nvt) - 27)]

    for i in range(0, len(df) - len(nvt_classic)):
        nvt_classic.insert(0, None)
    df['nvt_classic'] = nvt_classic
    df['nvt_classic'] = df['nvt_classic'].round(3)

    return df
