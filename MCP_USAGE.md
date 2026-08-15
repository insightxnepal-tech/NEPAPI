# NEPSE API MCP Server Usage Guide

This document provides a detailed guide on how to use the NEPSE API MCP (Model-driven Communication Protocol) Server. The server exposes a set of tools that can be used to access real-time and historical data from the Nepal Stock Exchange (NEPSE).

## Table of Contents

- [Introduction](#introduction)
- [Available Tools](#available-tools)
- [Tool Details](#tool-details)
  - [General Tools](#general-tools)
  - [Market Data Tools](#market-data-tools)
  - [Index and Sub-Index Tools](#index-and-sub-index-tools)
  - [Graph Data Tools](#graph-data-tools)
  - [Company and Security Tools](#company-and-security-tools)
- [Pydantic Models](#pydantic-models)
- [Available Prompts](#available-prompts)

## Introduction

The MCP server provides a structured way to interact with the NEPSE API. It uses a tool-based architecture, where each tool corresponds to a specific API endpoint or a set of related endpoints. The server uses Pydantic models to validate data and provide clear, consistent data structures in the output.

## Available Tools

Here is a list of all the tools available on the MCP server:

- `ping`
- `get_market_status`
- `get_market_summary`
- `get_nepse_subindex`
- `get_nepse_index`
- `get_daily_nepse_index_graph`
- `get_daily_sensitive_index_graph`
- `get_daily_float_index_graph`
- `get_daily_sensitive_float_index_graph`
- `get_daily_bank_subindex_graph`
- `get_daily_development_bank_subindex_graph`
- `get_daily_finance_subindex_graph`
- `get_daily_hotel_tourism_subindex_graph`
- `get_daily_hydropower_subindex_graph`
- `get_daily_investment_subindex_graph`
- `get_daily_life_insurance_subindex_graph`
- `get_daily_manufacturing_processing_subindex_graph`
- `get_daily_microfinance_subindex_graph`
- `get_daily_mutual_fund_subindex_graph`
- `get_daily_non_life_insurance_subindex_graph`
- `get_daily_others_subindex_graph`
- `get_daily_trading_subindex_graph`
- `get_live_market`
- `get_price_volume`
- `get_top_gainers`
- `get_top_losers`
- `get_top_ten_traded_by_shares`
- `get_top_ten_traded_by_turnover`
- `get_top_ten_traded_by_transactions`
- `get_supply_and_demand`
- `get_company_info`
- `get_financial_reports`
- `get_latest_financial_reports`
- `get_floorsheet`
- `get_historical_data`
- `get_market_depth`
- `get_security_overview`
- `validate_stock_symbol`
- `find_symbol_by_company_name`
- `find_company_name_by_symbol`

## Available Prompts

The MCP server also provides a set of pre-defined prompts to facilitate common user queries. These prompts are designed to be used by AI models to generate user-facing responses.

### `stock-quick-lookup`
- **Description**: Get a quick summary of a stock's current price, volume, and latest trades.
- **Parameters**:
    - `symbol` (str): The stock symbol.

### `market-sentiment-snapshot`
- **Description**: Get a snapshot of today's top gainers, losers, and overall market mood.
- **Parameters**: None

### `sector-performance`
- **Description**: Analyze the performance of a specific sector today.
- **Parameters**:
    - `sector` (str): The name of the sector.

### `company-deep-dive`
- **Description**: Get a detailed report on a company: profile, price history, and recent trades.
- **Parameters**:
    - `symbol` (str): The stock symbol.

### `live-market-watchlist`
- **Description**: Monitor live prices and volumes for a custom list of stocks.
- **Parameters**:
    - `symbols` (str): A comma-separated list of stock symbols.

### `market-depth-analyzer`
- **Description**: Analyze the current bid/ask depth for a stock (only when the market is open).
- **Parameters**:
    - `symbol` (str): The stock symbol.

### `post-market-trade-explorer`
- **Description**: Explore all trades for a stock after the market has closed (floorsheet).
- **Parameters**:
    - `symbol` (str): The stock symbol.

### `validate-stock-symbol`
- **Description**: Check if a stock symbol is valid and get suggestions if not.
- **Parameters**:
    - `symbol` (str): The stock symbol to validate.

### `market-open-status`
- **Description**: Check if the NEPSE market is currently open or closed.
- **Parameters**: None

### `setup-alert`
- **Description**: Set up a price or volume alert for a stock.
- **Parameters**:
    - `symbol` (str): The stock symbol.
    - `type` (str): The type of alert (e.g., "price", "volume").
    - `threshold` (str): The threshold value for the alert.

## Tool Details

### General Tools

#### `ping`
- **Description**: A simple tool to check if the server is running.
- **Parameters**: None
- **Output**: `{"pong": true}`

#### `get_market_status`
- **Description**: Get the current status of the NEPSE market.
- **Parameters**: None
- **Output**: A `MarketStatus` object.

### Market Data Tools

#### `get_market_summary`
- **Description**: Get the latest live NEPSE market summary.
- **Parameters**: None
- **Output**: A `Summary` object.

#### `get_live_market`
- **Description**: Get real-time live market data for all securities.
- **Parameters**:
    - `limit` (optional, int): Number of results per page.
    - `page` (optional, int): Page number for pagination.
- **Output**: A paginated list of `LiveMarketItem` objects.

#### `get_price_volume`
- **Description**: Get price and volume data for all stocks, or filter by company.
- **Parameters**:
    - `company` (optional, str): Company name or symbol to filter by.
    - `limit` (optional, int): Number of results per page.
    - `page` (optional, int): Page number for pagination.
- **Output**: A paginated list of `PriceVolumeItem` objects.

#### `get_top_gainers`
- **Description**: Get the top 10 gainers of the day.
- **Parameters**: None
- **Output**: A list of `TopGainerLoser` objects.

#### `get_top_losers`
- **Description**: Get the top 10 losers of the day.
- **Parameters**: None
- **Output**: A list of `TopGainerLoser` objects.

#### `get_top_ten_traded_by_shares`
- **Description**: Get the top 10 most traded scrips by shares.
- **Parameters**: None
- **Output**: A list of `TopTradeScrip` objects.

#### `get_top_ten_traded_by_turnover`
- **Description**: Get the top 10 most traded scrips by turnover.
- **Parameters**: None
- **Output**: A list of `TopTurnover` objects.

#### `get_top_ten_traded_by_transactions`
- **Description**: Get the top 10 most traded scrips by transactions.
- **Parameters**: None
- **Output**: A list of `TopTraders` objects.

#### `get_supply_and_demand`
- **Description**: Get supply and demand data.
- **Parameters**: None
- **Output**: A `SupplyDemandData` object.

#### `get_floorsheet`
- **Description**: Get the floorsheet (all trades) for a specific stock.
- **Parameters**:
    - `symbol` (str): The stock symbol.
    - `limit` (optional, int): Number of results per page.
    - `page` (optional, int): Page number for pagination.
- **Output**: A paginated list of `TradeContract` objects.

#### `get_market_depth`
- **Description**: Get the market depth (buy/sell orders) for a stock.
- **Parameters**:
    - `symbol` (str): The stock symbol.
- **Output**: A `MarketDepthResponse` object.

### Index and Sub-Index Tools

#### `get_nepse_index`
- **Description**: Get the NEPSE index and related indices.
- **Parameters**: None
- **Output**: A dictionary of `IndexData` objects, keyed by index name.

#### `get_nepse_subindex`
- **Description**: Get all NEPSE sub-indices (sector indices).
- **Parameters**: None
- **Output**: A dictionary of `SubIndex` objects, keyed by sub-index name.

### Graph Data Tools
These tools return time-series data for various indices.
- **Parameters**:
    - `limit` (optional, int): Number of results per page.
    - `page` (optional, int): Page number for pagination.
- **Output**: A paginated list of `TimeValue` objects.

- `get_daily_nepse_index_graph`
- `get_daily_sensitive_index_graph`
- `get_daily_float_index_graph`
- `get_daily_sensitive_float_index_graph`
- `get_daily_bank_subindex_graph`
- `get_daily_development_bank_subindex_graph`
- `get_daily_finance_subindex_graph`
- `get_daily_hotel_tourism_subindex_graph`
- `get_daily_hydropower_subindex_graph`
- `get_daily_investment_subindex_graph`
- `get_daily_life_insurance_subindex_graph`
- `get_daily_manufacturing_processing_subindex_graph`
- `get_daily_microfinance_subindex_graph`
- `get_daily_mutual_fund_subindex_graph`
- `get_daily_non_life_insurance_subindex_graph`
- `get_daily_others_subindex_graph`
- `get_daily_trading_subindex_graph`

### Company and Security Tools

#### `get_company_info`
- **Description**: Get detailed information about a company.
- **Parameters**:
    - `symbol` (str): The stock symbol.
- **Output**: A `CompanyInfo` object.

#### `get_financial_reports`
- **Description**: Get compact quarterly and annual financial reports for a listed stock, including EPS, P/E, profit, net worth per share, and NEPSE document links.
- **Parameters**:
    - `symbol` (str): The stock symbol.
- **Output**: A dictionary with `latestQuarterly`, `latestAnnual`, and `reports`.

#### `get_latest_financial_reports`
- **Description**: Get the latest quarterly and annual filings for one stock, one sector, or all active NEPSE equity listings.
- **Parameters**:
    - `symbol` (optional, str): Limit to one stock.
    - `sector` (optional, str): Limit to one sector name.
- **Output**: A dictionary with `count` and `results`.

#### `get_historical_data`
- **Description**: Get historical trade data for a stock.
- **Parameters**:
    - `symbol` (str): The stock symbol.
    - `start_date` (str): The start date (YYYY-MM-DD).
    - `end_date` (str): The end date (YYYY-MM-DD).
- **Output**: A list of `HistoricalTradeEntry` objects.

#### `get_security_overview`
- **Description**: Get a comprehensive overview of a security.
- **Parameters**:
    - `symbol` (str): The stock symbol.
- **Output**: A `SecurityOverview` object.

#### `validate_stock_symbol`
- **Description**: Validate a stock symbol.
- **Parameters**:
    - `symbol` (str): The stock symbol to validate.
- **Output**: A dictionary with validation results.

#### `find_symbol_by_company_name`
- **Description**: Find a stock symbol by company name.
- **Parameters**:
    - `company_name` (str): The name of the company.
- **Output**: A dictionary with the symbol and other info.

#### `find_company_name_by_symbol`
- **Description**: Find a company name by stock symbol.
- **Parameters**:
    - `symbol` (str): The stock symbol.
- **Output**: A dictionary with the company name and other info.

## Pydantic Models

The MCP server uses the following Pydantic models to structure the data returned by the tools.

- `Summary`
- `PriceVolumeItem`
- `SupplyDemand`
- `SupplyDemandData`
- `TopGainerLoser`
- `TopTradeScrip`
- `TopTurnover`
- `TopTraders`
- `MarketStatus`
- `CompanyInfo`
- `LiveMarketItem`
- `MarketIndex`
- `SubIndex`
- `TradeContract`
- `HistoricalTradeEntry`
- `MarketDepthItem`
- `MarketDepthData`
- `MarketDepthResponse`
- `SecurityDailyTradeDto`
- `InstrumentType`
- `ShareGroup`
- `SectorMaster`
- `CompanyId`
- `Security`
- `SecurityOverview`
- `TurnoverIndex`
- `ScripDetail`
- `SectorDetail`
- `MarketSummary`
- `IndexData`
- `TimeValue`
- `TimeSeriesData`
