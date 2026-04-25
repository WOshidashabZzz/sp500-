import os
import time
from io import StringIO

import pandas as pd
import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 关闭 verify=False 的告警
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# 启动时清理可能影响 requests 的代理环境变量
# =========================
for k in [
    "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
    "http_proxy", "https_proxy", "all_proxy"
]:
    os.environ.pop(k, None)

# =========================
# 推送配置
# =========================
PUSHPLUS_TOKEN = os.getenv("PUSHPLUS_TOKEN", "")

# =========================
# 策略配置
# =========================
SYMBOL = "SPY"
CACHE_FILE = "sp500_cache.csv"

MA_WINDOW = 20
A_INVEST = 50.0
MIN_TOTAL = 60.0
MAX_TOTAL = 100.0
EXPONENT = 1.5
FULL_BUY_DEVIATION = 0.08
SHOW_RECENT_ROWS = 10
# =========================


# ========= 通用请求 =========
def make_session():
    session = requests.Session()
    session.trust_env = False  # 忽略系统代理环境变量
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Accept": "*/*",
        "Connection": "close",
    })

    retries = Retry(
        total=2,
        connect=2,
        read=2,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


# ========= 推送 =========
def send_push(title, content, retries=3, delay=3):
    token = PUSHPLUS_TOKEN.strip()
    if not token or "token" in token.lower():
        print("未配置有效 PUSHPLUS_TOKEN，跳过推送")
        return False

    url = "https://www.pushplus.plus/send"
    session = make_session()

    data = {
        "token": token,
        "title": title,
        "content": content
    }

    last_err = None
    for i in range(retries):
        try:
            r = session.post(url, json=data, timeout=15, verify=False)
            print("推送结果:", r.text)
            result = r.json() if r.text else {}
            return result.get("code") == 200
        except Exception as e:
            last_err = e
            print(f"第{i+1}次推送失败:", e)
            time.sleep(delay)

    print("推送最终失败:", last_err)
    return False


def push_today_signal(row, source_name):
    content = f"""
📊 今日定投建议

📡 数据源: {source_name}
📅 数据日期: {row['date'].strftime('%Y-%m-%d')}
📉 低于均值: {max(0, row['deviation_pct']):.2f}%

💰 A类: {A_INVEST:.2f}
🚀 C类: {row['c_invest']:.2f}

🧮 总投入: {row['invest']:.2f}
"""
    return send_push("标普定投提醒", content)


# ========= 数据源1：Stooq HTTP =========
def load_data_from_stooq_http():
    ts = int(time.time())
    url = f"http://stooq.com/q/d/l/?s=spy.us&i=d&nocache={ts}"
    session = make_session()

    resp = session.get(url, timeout=20)
    resp.raise_for_status()

    text = resp.text.strip()
    if not text:
        raise ValueError("Stooq HTTP 返回空内容")

    df = pd.read_csv(StringIO(text))

    if "Date" not in df.columns or "Close" not in df.columns:
        raise ValueError(f"Stooq HTTP 列名异常: {list(df.columns)}")

    df["date"] = pd.to_datetime(df["Date"])
    df["price"] = pd.to_numeric(df["Close"], errors="coerce")
    df = df[["date", "price"]].dropna().sort_values("date").reset_index(drop=True)

    if df.empty:
        raise ValueError("Stooq HTTP 返回空数据")

    print("Stooq HTTP 最新日期:", df.iloc[-1]["date"].strftime("%Y-%m-%d"))
    return df, "Stooq HTTP"


# ========= 数据源2：Stooq HTTPS =========
def load_data_from_stooq_https():
    ts = int(time.time())
    url = f"https://stooq.com/q/d/l/?s=spy.us&i=d&nocache={ts}"
    session = make_session()

    resp = session.get(url, timeout=20, verify=False)
    resp.raise_for_status()

    text = resp.text.strip()
    if not text:
        raise ValueError("Stooq HTTPS 返回空内容")

    df = pd.read_csv(StringIO(text))

    if "Date" not in df.columns or "Close" not in df.columns:
        raise ValueError(f"Stooq HTTPS 列名异常: {list(df.columns)}")

    df["date"] = pd.to_datetime(df["Date"])
    df["price"] = pd.to_numeric(df["Close"], errors="coerce")
    df = df[["date", "price"]].dropna().sort_values("date").reset_index(drop=True)

    if df.empty:
        raise ValueError("Stooq HTTPS 返回空数据")

    print("Stooq HTTPS 最新日期:", df.iloc[-1]["date"].strftime("%Y-%m-%d"))
    return df, "Stooq HTTPS"


# ========= 数据源3：Yahoo Chart =========
def load_data_from_yahoo():
    url = (
        "https://query1.finance.yahoo.com/v8/finance/chart/SPY"
        "?range=6mo&interval=1d&includePrePost=false&events=div%2Csplits"
    )
    session = make_session()

    resp = session.get(url, timeout=20, verify=False)
    resp.raise_for_status()
    data = resp.json()

    result = data["chart"]["result"][0]
    timestamps = result["timestamp"]
    closes = result["indicators"]["quote"][0]["close"]

    df = pd.DataFrame({
        "date": pd.to_datetime(timestamps, unit="s"),
        "price": closes
    })

    df = df.dropna().sort_values("date").reset_index(drop=True)

    if df.empty:
        raise ValueError("Yahoo Chart 返回空数据")

    print("Yahoo 最新日期:", df.iloc[-1]["date"].strftime("%Y-%m-%d"))
    return df, "Yahoo Chart"


def load_data_from_nasdaq():
    from datetime import date, timedelta

    end_date = date.today()
    start_date = end_date - timedelta(days=365)
    url = (
        "https://api.nasdaq.com/api/quote/SPY/historical"
        f"?assetclass=etf&fromdate={start_date:%Y-%m-%d}"
        f"&todate={end_date:%Y-%m-%d}&limit=400"
    )
    session = make_session()
    session.headers.update({
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.nasdaq.com/market-activity/etf/spy/historical",
        "Origin": "https://www.nasdaq.com",
    })

    resp = session.get(url, timeout=25, verify=False)
    resp.raise_for_status()
    payload = resp.json()

    rows = (payload.get("data") or {}).get("tradesTable", {}).get("rows", [])
    if not rows:
        raise ValueError("Nasdaq 返回空数据")

    records = []
    for row in rows:
        d = row.get("date")
        c = str(row.get("close", "")).replace(",", "").replace("$", "")
        records.append({"date": d, "price": c})

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"], format="%m/%d/%Y", errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna().sort_values("date").reset_index(drop=True)

    if df.empty:
        raise ValueError("Nasdaq 解析后为空")

    print("Nasdaq 最新日期:", df.iloc[-1]["date"].strftime("%Y-%m-%d"))
    return df, "Nasdaq ETF"


def load_data_from_fred():
    url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=SP500"
    session = make_session()

    resp = session.get(url, timeout=30, verify=False)
    resp.raise_for_status()

    text = resp.text.strip()
    if not text:
        raise ValueError("FRED 返回空内容")

    df = pd.read_csv(StringIO(text))
    if "observation_date" not in df.columns or "SP500" not in df.columns:
        raise ValueError(f"FRED 列名异常: {list(df.columns)}")

    df["date"] = pd.to_datetime(df["observation_date"], errors="coerce")
    df["price"] = pd.to_numeric(df["SP500"], errors="coerce")
    df = df[["date", "price"]].dropna().sort_values("date").reset_index(drop=True)

    if df.empty:
        raise ValueError("FRED 返回空数据")

    print("FRED 最新日期:", df.iloc[-1]["date"].strftime("%Y-%m-%d"))
    return df, "FRED SP500"


def load_data_from_stooq_quote():
    url = "https://stooq.com/q/l/?s=spy.us&f=sd2t2ohlcv&h&e=csv"
    session = make_session()

    resp = session.get(url, timeout=20, verify=False)
    resp.raise_for_status()

    text = resp.text.strip()
    if not text:
        raise ValueError("Stooq Quote 返回空内容")

    df = pd.read_csv(StringIO(text))
    if "Date" not in df.columns or "Close" not in df.columns:
        raise ValueError(f"Stooq Quote 列名异常: {list(df.columns)}")

    df["date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["price"] = pd.to_numeric(df["Close"], errors="coerce")
    df = df[["date", "price"]].dropna().sort_values("date").reset_index(drop=True)

    if df.empty:
        raise ValueError("Stooq Quote 返回空数据")

    print("Stooq Quote 最新日期:", df.iloc[-1]["date"].strftime("%Y-%m-%d"))
    return df, "Stooq Quote"


# ========= 缓存 =========
def save_cache(df, source_name):
    out = df[["date", "price"]].copy()
    if os.path.exists(CACHE_FILE):
        try:
            old = pd.read_csv(CACHE_FILE)
            if "date" in old.columns and "price" in old.columns:
                old["date"] = pd.to_datetime(old["date"], errors="coerce")
                old["price"] = pd.to_numeric(old["price"], errors="coerce")
                old = old[["date", "price"]].dropna()
                out = pd.concat([old, out], ignore_index=True)
        except Exception:
            pass

    out = out.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    out["source"] = source_name
    out.to_csv(CACHE_FILE, index=False)


def load_cache():
    if not os.path.exists(CACHE_FILE):
        raise FileNotFoundError(f"缓存文件不存在: {CACHE_FILE}")

    df = pd.read_csv(CACHE_FILE)
    df["date"] = pd.to_datetime(df["date"])
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    source_name = df["source"].iloc[0] if "source" in df.columns else "本地缓存"

    df = df[["date", "price"]].dropna().sort_values("date").reset_index(drop=True)

    if df.empty:
        raise ValueError("缓存文件为空")

    print("缓存最新日期:", df.iloc[-1]["date"].strftime("%Y-%m-%d"))
    return df, source_name + "（缓存）"


# ========= 刷新入口 =========
def refresh_data():
    # 1. Stooq HTTP
    try:
        df, source_name = load_data_from_stooq_http()
        save_cache(df, source_name)
        print("已刷新最新数据，来源:", source_name)
        return df, source_name
    except Exception as e:
        print("Stooq HTTP 刷新失败：", e)

    # 2. Stooq HTTPS
    try:
        df, source_name = load_data_from_stooq_https()
        save_cache(df, source_name)
        print("已刷新最新数据，来源:", source_name)
        return df, source_name
    except Exception as e:
        print("Stooq HTTPS 刷新失败：", e)

    # 3. Yahoo Chart
    try:
        df, source_name = load_data_from_yahoo()
        save_cache(df, source_name)
        print("已刷新最新数据，来源:", source_name)
        return df, source_name
    except Exception as e:
        print("Yahoo 刷新失败：", e)

    # 4. Nasdaq 历史
    try:
        df, source_name = load_data_from_nasdaq()
        save_cache(df, source_name)
        try:
            quote_df, quote_source = load_data_from_stooq_quote()
            save_cache(quote_df, quote_source)
            merged_df, _ = load_cache()
            print("已刷新最新数据，来源:", f"{source_name} + {quote_source}")
            return merged_df, f"{source_name} + {quote_source}"
        except Exception as quote_err:
            print("Nasdaq后补当日价失败：", quote_err)
        print("已刷新最新数据，来源:", source_name)
        return df, source_name
    except Exception as e:
        print("Nasdaq 刷新失败：", e)

    # 5. FRED 历史
    try:
        df, source_name = load_data_from_fred()
        save_cache(df, source_name)
        print("已刷新最新数据，来源:", source_name)
        return df, source_name
    except Exception as e:
        print("FRED 刷新失败：", e)

    # 6. Stooq Quote 当日
    try:
        df, source_name = load_data_from_stooq_quote()
        save_cache(df, source_name)
        merged_df, _ = load_cache()
        print("已刷新最新数据，来源:", source_name)
        return merged_df, source_name
    except Exception as e:
        print("Stooq Quote 刷新失败：", e)

    # 7. 缓存
    print("外部数据源全部失败，改用缓存数据")
    df, source_name = load_cache()
    return df, source_name


# ========= 策略 =========
def calc_total_invest(price, ma):
    if pd.isna(ma) or ma <= 0:
        return MIN_TOTAL

    deviation = max(0.0, (ma - price) / ma)
    ratio = min(1.0, deviation / FULL_BUY_DEVIATION)

    invest = MIN_TOTAL + (MAX_TOTAL - MIN_TOTAL) * (ratio ** EXPONENT)
    return round(invest, 2)


def build_signal_table(df):
    out = df.copy()

    out["ma"] = out["price"].rolling(MA_WINDOW).mean()
    out["deviation_pct"] = ((out["ma"] - out["price"]) / out["ma"]) * 100
    out["deviation_pct"] = out["deviation_pct"].fillna(0.0)

    out["invest"] = out.apply(
        lambda row: calc_total_invest(row["price"], row["ma"]),
        axis=1
    )

    out["c_invest"] = (out["invest"] - A_INVEST).clip(lower=10, upper=50)
    return out


# ========= 输出 =========
def print_today(signal_df, source_name):
    row = signal_df.iloc[-1]

    print("\n=========== 今日建议 ===========")
    print(f"数据源: {source_name}")
    print(f"数据日期: {row['date'].strftime('%Y-%m-%d')}")
    print(f"当前价格: {row['price']:.2f}")

    if pd.notna(row["ma"]):
        print(f"{MA_WINDOW}日均值: {row['ma']:.2f}")
    else:
        print(f"{MA_WINDOW}日均值: 数据不足")

    print(f"低于均值: {max(0, row['deviation_pct']):.2f}%")
    print(f"\nA类固定: {A_INVEST:.2f}")
    print(f"C类加仓: {row['c_invest']:.2f}")
    print(f"总投入: {row['invest']:.2f}")
    print("================================\n")


def print_table(signal_df):
    recent = signal_df.tail(SHOW_RECENT_ROWS).copy()

    recent["date"] = recent["date"].dt.strftime("%Y-%m-%d")
    recent["price"] = recent["price"].map(lambda x: f"{x:.2f}")
    recent["ma"] = recent["ma"].map(lambda x: "-" if pd.isna(x) else f"{x:.2f}")
    recent["deviation_pct"] = recent["deviation_pct"].map(lambda x: f"{max(0, x):.2f}%")
    recent["c_invest"] = recent["c_invest"].map(lambda x: f"{x:.2f}")

    print("最近信号：")
    print(recent[["date", "price", "ma", "deviation_pct", "c_invest"]].to_string(index=False))


# ========= 主函数 =========
def main():
    try:
        df, source_name = refresh_data()

        signal_df = build_signal_table(df)

        print_today(signal_df, source_name)
        print_table(signal_df)

        row = signal_df.iloc[-1]
        pushed = push_today_signal(row, source_name)
        print("本次已推送" if pushed else "本次未推送")

    except Exception as e:
        print("运行失败：", e)


if __name__ == "__main__":
    main()
