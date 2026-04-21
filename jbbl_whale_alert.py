import pandas as pd
import glob
import os
import re
import requests
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
from pathlib import Path

# --- CONFIGURATION ---
TELEGRAM_TOKEN = "8618135314:AAHoDrHGP2sncP1HxEGLDj0OKtIpSLeuD0U"
TELEGRAM_CHAT_ID = "8563709547"
EMAIL_RECEIVER = "insightxnepal@gmail.com"
# ---------------------

def get_latest_floorsheet():
    paths = [
        "/Users/sanishtamang/NEPAPI/floorsheet*.csv",
        "/Users/sanishtamang/Downloads/floorsheet/floorsheet*.csv"
    ]
    all_files = []
    for p in paths:
        all_files.extend(glob.glob(p))
    
    if not all_files:
        return None
        
    def extract_date(f):
        match = re.search(r'(\d{4})[_-](\d{2})[_-](\d{2})', f)
        return "-".join(match.groups()) if match else "0000-00-00"
    
    all_files.sort(key=extract_date, reverse=True)
    return all_files[0]

def send_telegram_alert(message):
    if TELEGRAM_TOKEN == "PASTE_YOUR_TOKEN_HERE":
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    try:
        requests.post(url, json=payload)
    except Exception as e:
        print(f"Error sending Telegram: {e}")

def send_email_alert(subject, message):
    # This is a placeholder. User needs to provide an App Password for Gmail!
    return

def check_jbbl_whale_alert():
    symbol = "JBBL"
    whale_id = 58
    log_file = "/Users/sanishtamang/NEPAPI/whale_alerts.log"
    
    latest_file = get_latest_floorsheet()
    if not latest_file:
        return

    filename = os.path.basename(latest_file)
    
    # Check if we already logged this file
    if os.path.exists(log_file):
        with open(log_file, "r") as f:
            if filename in f.read():
                return

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df = pd.read_csv(latest_file)
    df_sym = df[df['stockSymbol'] == symbol]
    
    if df_sym.empty:
        log_entry = f"[{ts}] File: {filename} | No trades found for {symbol}\n"
    else:
        bought = df_sym[df_sym['buyerMemberId'] == whale_id]['contractQuantity'].sum()
        sold = df_sym[df_sym['sellerMemberId'] == whale_id]['contractQuantity'].sum()
        net = bought - sold
        
        status = "NEUTRAL"
        alert_msg = None
        
        if net < -5000:
            status = "🚨 RED ALERT (SELLING)"
            alert_msg = f"JBBL WHALE ALERT: Broker {whale_id} SOLD {abs(net):,} units today! (File: {filename})"
        elif net > 5000:
            status = "✅ ACCUMULATING"
            alert_msg = f"JBBL WHALE UPDATE: Broker {whale_id} bought another {net:,} units. Trend continues."
            
        log_entry = f"[{ts}] File: {filename} | Broker {whale_id}: {net:+,} units | Status: {status}\n"
        
        if alert_msg:
            send_telegram_alert(alert_msg)

    with open(log_file, "a") as f:
        f.write(log_entry)
    
    print(log_entry.strip())

if __name__ == "__main__":
    check_jbbl_whale_alert()
