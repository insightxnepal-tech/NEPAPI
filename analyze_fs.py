import pandas as pd
import json

def analyze():
    print("Loading floorsheet data...")
    df = pd.read_csv('floorsheet_2026_03_25.csv')
    
    print("Computing metrics...")
    insights = {
        'Total Turnover (Rs)': float(df['contractAmount'].sum()),
        'Total Volume (Shares)': int(df['contractQuantity'].sum()),
        'Total Transactions': len(df),
        'Top 5 Stocks by Turnover': df.groupby('stockSymbol')['contractAmount'].sum().nlargest(5).to_dict(),
        'Top 5 Stocks by Volume': df.groupby('stockSymbol')['contractQuantity'].sum().nlargest(5).to_dict(),
        'Top 5 Stocks by Transactions': df['stockSymbol'].value_counts().nlargest(5).to_dict(),
        'Top 5 Buyer Brokers by Amount': df.groupby('buyerBrokerName')['contractAmount'].sum().nlargest(5).to_dict(),
        'Top 5 Seller Brokers by Amount': df.groupby('sellerBrokerName')['contractAmount'].sum().nlargest(5).to_dict()
    }
    
    with open('insights.json', 'w') as f:
        json.dump(insights, f, indent=4)
        
    print("Analysis complete. Check insights.json")

if __name__ == '__main__':
    analyze()
