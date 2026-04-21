import pandas as pd
from pathlib import Path

def analyze_stock_dynamics(symbol, df_fs):
    print(f"\n--- Deep Dive: {symbol} ---")
    
    # Filter trades for the symbol
    df_stock = df_fs[df_fs['stockSymbol'] == symbol].copy()
    
    if df_stock.empty:
        print(f"No trade data found for {symbol} in the provided floorsheet.")
        return

    total_qty = df_stock['contractQuantity'].sum()
    total_amt = df_stock['contractAmount'].sum()
    vwap = total_amt / total_qty
    
    print(f"Total Volume:  {total_qty:,} units")
    print(f"Traded VWAP:   Rs. {vwap:.2f}")
    
    # Broker Analysis
    buyer_stats = df_stock.groupby('buyerMemberId')['contractQuantity'].sum()
    seller_stats = df_stock.groupby('sellerMemberId')['contractQuantity'].sum()
    
    all_brokers = sorted(list(set(buyer_stats.index) | set(seller_stats.index)))
    broker_summary = []
    
    for b in all_brokers:
        bought = buyer_stats.get(b, 0)
        sold = seller_stats.get(b, 0)
        net = bought - sold
        broker_summary.append({
            'Broker': b,
            'Bought': bought,
            'Sold': sold,
            'Net': net
        })
        
    df_brokers = pd.DataFrame(broker_summary)
    
    # Top Accumulators (Net Buyers)
    top_buyers = df_brokers.sort_values('Net', ascending=False).head(5)
    print("\nTop Net Buyers (Accumulation):")
    for _, row in top_buyers.iterrows():
        if row['Net'] > 0:
            print(f"  Broker {int(row['Broker'])}: +{int(row['Net']):,} units ({row['Bought']:,} B / {row['Sold']:,} S)")
            
    # Top Sellers (Distribution)
    top_sellers = df_brokers.sort_values('Net', ascending=True).head(5)
    print("\nTop Net Sellers (Distribution):")
    for _, row in top_sellers.iterrows():
        if row['Net'] < 0:
            print(f"  Broker {int(row['Broker'])}: {int(row['Net']):,} units ({row['Bought']:,} B / {row['Sold']:,} S)")

    # Whale Trades
    whale_threshold = 2000 # Adjust based on stock liquidity
    whales = df_stock[df_stock['contractQuantity'] >= whale_threshold]
    if not whales.empty:
        print(f"\nWhale Activity (Trades >= {whale_threshold} units):")
        for _, row in whales.head(5).iterrows():
            print(f"  {row['contractQuantity']:,} @ Rs.{row['contractRate']} (Buyer: {row['buyerMemberId']} -> Seller: {row['sellerMemberId']})")
    else:
        print("\nNo major whale trades detected (>2k units).")

def main():
    floorsheet_path = Path("/Users/sanishtamang/NEPAPI/floorsheet_2026-04-18.csv")
    df_fs = pd.read_csv(floorsheet_path)
    
    symbols_to_analyze = ['HLI', 'GBIME']
    
    print("="*60)
    print("INSTITUTIONAL ACCUMULATION/DISTRIBUTION ANALYSIS")
    print("Session Date: 2026-04-17")
    print("="*60)
    
    for sym in symbols_to_analyze:
        analyze_stock_dynamics(sym, df_fs)
    print("\n" + "="*60)

if __name__ == "__main__":
    main()
