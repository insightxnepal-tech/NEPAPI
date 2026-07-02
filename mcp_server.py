import logging
import os
import time
import threading
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ValidationError
from starlette.middleware.authentication import AuthenticationMiddleware

import httpx
from dotenv import load_dotenv
from fastmcp import FastMCP
from fastmcp.prompts.prompt import PromptMessage, TextContent
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from fastmcp.server.middleware.rate_limiting import (
    RateLimitingMiddleware,
)
from fastmcp.server.middleware.error_handling import (
    ErrorHandlingMiddleware,
    RetryMiddleware
)

load_dotenv()

# Import validation utilities
from validator import validate_stock_symbol, find_symbol_by_company_name, find_company_name_by_symbol

BASE_URL = os.environ.get("BASE_URL", "http://localhost:8000")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

PORT = int(os.environ.get("PORT", 9000))


mcp = FastMCP(
    name="nepseapi-mcp-server",
    on_duplicate="error",
)

app = mcp.http_app()

# --- Rate Limiting Middleware ---
mcp.add_middleware(RateLimitingMiddleware(
    max_requests_per_second=60/60,  # 60 per minute # change rate limit from here
    burst_capacity=6
))

mcp.add_middleware(ErrorHandlingMiddleware(
    include_traceback=True,
    transform_errors=True,
    error_callback=lambda request, exc: logger.error(f"Error in MCP server: {exc}"),
))

mcp.add_middleware(RetryMiddleware(
    max_retries=3,
    retry_exceptions=(ConnectionError, TimeoutError)
))

@mcp.prompt(
    name="stock-quick-lookup",
    description="Get a quick summary of a stock's current price, volume, and latest trades."
)
def stock_quick_lookup(symbol: str) -> PromptMessage:
    """Get a quick summary of a stock's current price, volume, and latest trades."""
    return PromptMessage(role="user", content=TextContent(type="text", text=f"Show me a quick summary for {symbol}."))

@mcp.prompt(
    name="market-sentiment-snapshot",
    description="Get a snapshot of today's top gainers, losers, and overall market mood."
)
def market_sentiment_snapshot() -> PromptMessage:
    """Get a snapshot of today's top gainers, losers, and overall market mood."""
    return PromptMessage(role="user", content=TextContent(type="text", text="Give me today's top gainers, losers, and a market summary."))

@mcp.prompt(
    name="sector-performance",
    description="Analyze the performance of a specific sector today."
)
def sector_performance(sector: str) -> PromptMessage:
    """Analyze the performance of a specific sector today."""
    return PromptMessage(role="user", content=TextContent(type="text", text=f"Analyze today's performance for the {sector} sector."))

@mcp.prompt(
    name="company-deep-dive",
    description="Get a detailed report on a company: profile, price history, and recent trades."
)
def company_deep_dive(symbol: str) -> PromptMessage:
    """Get a detailed report on a company: profile, price history, and recent trades."""
    return PromptMessage(role="user", content=TextContent(type="text", text=f"Give me a detailed report for {symbol}, including profile, price history, and recent trades."))

@mcp.prompt(
    name="live-market-watchlist",
    description="Monitor live prices and volumes for a custom list of stocks."
)
def live_market_watchlist(symbols: str) -> PromptMessage:
    """Monitor live prices and volumes for a custom list of stocks."""
    return PromptMessage(role="user", content=TextContent(type="text", text=f"Show me live prices and volumes for: {symbols}"))

@mcp.prompt(
    name="market-depth-analyzer",
    description="Analyze the current bid/ask depth for a stock (only when market is open)."
)
def market_depth_analyzer(symbol: str) -> PromptMessage:
    """Analyze the current bid/ask depth for a stock (only when market is open)."""
    return PromptMessage(role="user", content=TextContent(type="text", text=f"Analyze the current market depth for {symbol}."))

@mcp.prompt(
    name="post-market-trade-explorer",
    description="Explore all trades for a stock after market close (floorsheet)."
)
def post_market_trade_explorer(symbol: str) -> PromptMessage:
    """Explore all trades for a stock after market close (floorsheet)."""
    return PromptMessage(role="user", content=TextContent(type="text", text=f"Show me all trades for {symbol} after market close."))

@mcp.prompt(
    name="validate-stock-symbol",
    description="Check if a stock symbol is valid and get suggestions if not."
)
def validate_stock_symbol_prompt(symbol: str) -> PromptMessage:
    """Check if a stock symbol is valid and get suggestions if not."""
    return PromptMessage(role="user", content=TextContent(type="text", text=f"Validate the stock symbol: {symbol}"))

@mcp.prompt(
    name="market-open-status",
    description="Check if the NEPSE market is currently open or closed."
)
def market_open_status() -> PromptMessage:
    """Check if the NEPSE market is currently open or closed."""
    return PromptMessage(role="user", content=TextContent(type="text", text="Is the NEPSE market currently open or closed?"))

@mcp.prompt(
    name="setup-alert",
    description="Set up a price or volume alert for a stock (UI clients can use this to trigger notifications)."
)
def setup_alert(symbol: str, type: str, threshold: str) -> PromptMessage:
    """Set up a price or volume alert for a stock (UI clients can use this to trigger notifications)."""
    return PromptMessage(role="user", content=TextContent(type="text", text=f"Set up an alert for {symbol} when {type} crosses {threshold}."))

# Pydantic Models for Type Validation
class Summary(BaseModel):
    totalTurnoverRs: float = Field(..., alias="Total Turnover Rs:")
    totalTradedShares: float = Field(..., alias="Total Traded Shares")
    totalTransactions: float = Field(..., alias="Total Transactions")
    totalScripsTraded: float = Field(..., alias="Total Scrips Traded")

class PriceVolumeItem(BaseModel):
    securityId: str
    securityName: str
    symbol: str
    indexId: int
    totalTradeQuantity: int
    lastTradedPrice: float
    percentageChange: float
    previousClose: float
    closePrice: Optional[float] = None

class SupplyDemand(BaseModel):
    symbol: str
    totalOrder: int
    totalQuantity: int
    securityName: str
    securityId: Optional[str] = None

class SupplyDemandData(BaseModel):
    supplyList: List[SupplyDemand]
    demandList: List[SupplyDemand]

class TopGainerLoser(BaseModel):
    symbol: str
    ltp: float
    pointChange: float
    percentageChange: float
    securityName: str
    securityId: int

class TopTradeScrip(BaseModel):
    symbol: str
    shareTraded: int
    closingPrice: float
    securityName: str
    securityId: int

class TopTurnover(BaseModel):
    symbol: str
    turnover: float
    closingPrice: float
    securityName: str
    securityId: int

class TopTraders(BaseModel):
    securityId: int
    totalTrades: int
    lastTradedPrice: float
    securityName: str
    symbol: str

class TopTransactions(BaseModel):
  securityId: int
  totalTrades: int
  lastTradedPrice: float
  securityName: str
  symbol: str

class TopTransaction(BaseModel):
  securityId: int
  totalTrades: int
  lastTradedPrice: int
  securityName: str
  symbol: str

class MarketStatus(BaseModel):
    isOpen: str
    asOf: str
    id: int

class CompanyInfo(BaseModel):
    id: int
    companyName: str
    symbol: str
    securityName: str
    status: str
    companyEmail: str
    website: str
    sectorName: str
    regulatoryBody: str
    instrumentType: str

class LiveMarketItem(BaseModel):
    securityId: str
    securityName: str
    symbol: str
    indexId: int
    openPrice: float
    highPrice: float
    lowPrice: float
    totalTradeQuantity: int
    totalTradeValue: float
    lastTradedPrice: float
    percentageChange: float
    lastUpdatedDateTime: str
    lastTradedVolume: int
    previousClose: float
    averageTradedPrice: float
    # These fields are sometimes missing in the API response, so make them optional
    totalTradedVolume: Optional[int] = None
    numberOfTrades: Optional[int] = None

# Additional Pydantic Models for remaining tools
class MarketIndex(BaseModel):
    id: int
    auditId: Optional[int] = None
    exchangeIndexId: Optional[int] = None
    generatedTime: str
    index: str
    close: float
    high: float
    low: float
    previousClose: float
    change: float
    perChange: float
    fiftyTwoWeekHigh: float
    fiftyTwoWeekLow: float
    currentValue: float

class SubIndex(BaseModel):
    id: int
    index: str
    change: float
    perChange: float
    currentValue: float

class TradeContract(BaseModel):
    contractId: int
    stockSymbol: str
    buyerMemberId: str
    sellerMemberId: str
    contractQuantity: int
    contractRate: float
    contractAmount: float
    businessDate: str
    tradeBookId: int
    stockId: int
    buyerBrokerName: str
    sellerBrokerName: str
    tradeTime: str
    securityName: str

class HistoricalTradeEntry(BaseModel):
    businessDate: str
    totalTrades: int
    totalTradedQuantity: int
    totalTradedValue: float
    highPrice: float
    lowPrice: float
    closePrice: float

class MarketDepthItem(BaseModel):
    stockId: int
    orderBookOrderPrice: float
    quantity: int
    orderCount: int
    isBuy: int  # 1 or 2
    buy: bool
    sell: bool

class MarketDepthData(BaseModel):
    buyMarketDepthList: List[MarketDepthItem]
    sellMarketDepthList: List[MarketDepthItem]

class MarketDepthResponse(BaseModel):
    symbol: str
    totalBuyQty: int
    marketDepth: MarketDepthData
    totalSellQty: int
    timeStamp: Optional[int] = None

class SecurityDailyTradeDto(BaseModel):
    securityId: str
    openPrice: float
    highPrice: float
    lowPrice: float
    totalTradeQuantity: int
    totalTrades: int
    lastTradedPrice: float
    previousClose: float
    businessDate: str
    closePrice: float
    fiftyTwoWeekHigh: float
    fiftyTwoWeekLow: float
    lastUpdatedDateTime: str

class InstrumentType(BaseModel):
    id: int
    code: str
    description: str
    activeStatus: str

class ShareGroup(BaseModel):
    id: int
    name: str
    description: str
    capitalRangeMin: int
    modifiedBy: Optional[str] = None
    modifiedDate: Optional[str] = None
    activeStatus: str
    isDefault: str

class SectorMaster(BaseModel):
    id: int
    sectorDescription: str
    activeStatus: str
    regulatoryBody: str

class CompanyId(BaseModel):
    id: int
    companyShortName: str
    companyName: str
    email: str
    companyWebsite: str
    companyContactPerson: str
    sectorMaster: SectorMaster
    companyRegistrationNumber: str
    activeStatus: str

class Security(BaseModel):
    id: int
    symbol: str
    isin: str
    permittedToTrade: str
    listingDate: str
    creditRating: Optional[str] = None
    tickSize: float
    instrumentType: InstrumentType
    capitalGainBaseDate: str
    faceValue: float
    highRangeDPR: float
    issuerName: Optional[str] = None
    meInstanceNumber: int
    parentId: Optional[int] = None
    recordType: int
    schemeDescription: Optional[str] = None
    schemeName: Optional[str] = None
    secured: Optional[str] = None
    series: Optional[str] = None
    shareGroupId: ShareGroup
    activeStatus: str
    divisor: int
    cdsStockRefId: int
    securityName: str
    tradingStartDate: str
    networthBasePrice: float
    securityTradeCycle: int
    isPromoter: str
    companyId: CompanyId

class SecurityOverview(BaseModel):
    securityDailyTradeDto: SecurityDailyTradeDto
    security: Security
    stockListedShares: float
    paidUpCapital: float
    issuedCapital: float
    marketCapitalization: float
    publicShares: int
    publicPercentage: float
    promoterShares: float
    promoterPercentage: float
    updatedDate: str
    securityId: int

class TurnoverIndex(BaseModel):
    id: int
    index: str
    change: float
    perChange: float
    currentValue: float

class ScripDetail(BaseModel):
    symbol: str
    sector: str
    Turnover: float
    transaction: int
    volume: int
    previousClose: float
    lastUpdatedDateTime: int
    name: str
    category: str
    pointChange: float
    percentageChange: float
    ltp: float

class SectorDetail(BaseModel):
    transaction: int
    volume: int
    totalTurnover: float
    turnover: TurnoverIndex
    sectorName: str

class MarketSummary(BaseModel):
    scripsDetails: Dict[str, ScripDetail]
    sectorsDetails: Dict[str, SectorDetail]

class IndexData(BaseModel):
    id: int
    auditId: Optional[int]
    exchangeIndexId: Optional[int]
    generatedTime: Optional[str]
    index: str
    close: float
    high: float
    low: float
    previousClose: float
    change: float
    perChange: float
    fiftyTwoWeekHigh: float
    fiftyTwoWeekLow: float
    currentValue: float


from pydantic import RootModel

class NepseIndex(RootModel[Dict[str, IndexData]]):
    pass

class AllIndices(RootModel[Dict[str, IndexData]]):
    pass

class TimeValue(BaseModel):
    timestamp: int
    value: float


class TimeSeriesData(BaseModel):
    data: List[TimeValue]

    @classmethod
    def from_list(cls, raw: List[List[float]]) -> "TimeSeriesData":
        return cls(data=[TimeValue(timestamp=t[0], value=t[1]) for t in raw])

# Global cache for endpoint responses (thread-safe)
_endpoint_cache = {}
_endpoint_cache_lock = threading.Lock()
_ENDPOINT_CACHE_TTL = 600  # 10 minutes

def fetch_nepse_api(endpoint: str) -> Dict[str, Any]:
    """Fetch data from the NEPSE API and return parsed JSON, with endpoint-level caching."""
    now = time.time()
    cache_key = endpoint
    with _endpoint_cache_lock:
        cached = _endpoint_cache.get(cache_key)
        if cached:
            data, expires_at = cached
            if now < expires_at:
                return data
            else:
                del _endpoint_cache[cache_key]
    url = f"{BASE_URL}{endpoint}"
    response = httpx.get(url, timeout=30.0)
    response.raise_for_status()
    data = response.json()
    with _endpoint_cache_lock:
        _endpoint_cache[cache_key] = (data, now + _ENDPOINT_CACHE_TTL)
    return data

def validate_and_return(data: Any, model_class: BaseModel, is_list: bool = False):
    """Validate data against Pydantic model and return validated result."""
    try:
        if is_list:
            return [model_class(**item) for item in data]
        else:
            return model_class(**data)
    except ValidationError as e:
        logger.error(f"Validation error for {model_class.__name__}: {e}")
        return data  # Return raw data if validation fails

@mcp.tool()
def ping() -> Dict[str, bool]:
    return {"pong": True}

@mcp.tool()
def get_market_status() -> Dict[str, Any]:
    """
    Get the current status of the NEPSE market.
    Returns:
        Dict with keys:
            - isOpen: "OPEN" or "CLOSED"
            - asOf: Timestamp of the status
            - id: Status identifier
    Use this tool to check if the NEPSE market is currently open or closed before making live market or trading queries.
    """
    try:
        market_status_response = fetch_nepse_api("/IsNepseOpen")
        market_status = validate_and_return(market_status_response, MarketStatus)
        return market_status.model_dump() if hasattr(market_status, 'model_dump') else market_status_response
    except Exception as e:
        logger.error(f"Error fetching market status: {e}")
        return {"error": str(e)}


def check_market_open() -> bool:
    """
    Returns True if the NEPSE market is currently open, False if closed.
    Use this tool to programmatically check market status before calling live market or trading tools.
    """
    try:
        market_status_response = fetch_nepse_api("/IsNepseOpen")
        market_status = validate_and_return(market_status_response, MarketStatus)

        # Check if market is open using both validated model and raw response
        if hasattr(market_status, 'isOpen'):
            return market_status.isOpen == 'OPEN'
        else:
            return market_status_response.get('isOpen') == 'OPEN'
    except Exception as e:
        logger.error(f"Error checking market status: {e}")
        return False  # Assume market is closed if we can't check

@mcp.tool()
def get_market_summary() -> Dict[str, float]:
    """
    Get the latest live NEPSE market summary including key metrics.
    Returns:
        Dict with keys:
            - totalTurnoverRs: Total turnover in rupees
            - totalTradedShares: Total number of shares traded
            - totalTransactions: Total number of transactions
            - totalScripsTraded: Total number of scrips traded
    Use this tool for a quick overview of the day's market activity.
    """
    try:
        response = fetch_nepse_api("/Summary")
        validated_data = validate_and_return(response, Summary)
        return validated_data.model_dump() if hasattr(validated_data, 'model_dump') else response
    except Exception as e:
        logger.error(f"Error fetching market summary: {e}")
        return {"error": str(e)}

@mcp.tool()
def get_nepse_subindex() -> Dict:
    """
    Get all NEPSE subindices (sector indices).
    Use this to get the live performance of indexes like
    Development Bank Index
    Manufacturing And Processing
    Microfinance Index
    Life Insurance
    Mutual Fund
    Banking SubIndex
    Hotels And Tourism Index
    Others Index
    HydroPower Index
    Non Life Insurance
    Finance Index
    Trading Index
    Investment Index

    Returns:
        Dict mapping subindex names to their data:
            - id: Index ID
            - index: Name of the subindex
            - change: Absolute change in index value
            - perChange: Percentage change in index value
            - currentValue: Current value of the subindex
    Use this tool to get the latest values for all sector indices (e.g., Banking, HydroPower, Finance, etc.).
    """
    try:
        response = fetch_nepse_api("/NepseSubIndices")
        validated = validate_and_return(response, AllIndices)
        return validated.__root__ if hasattr(validated, "__root__") else response
    except Exception as e:
        logger.error(f"Error fetching NEPSE subindices: {e}")
        return {"error": str(e)}


@mcp.tool()
def get_nepse_index() -> Dict:
    """ Get the NEPSE index and related indices.
     Provides detailed live performance data for the these index.
     Sensitive Float Index
     Float Index
     Sensitive Index
     NEPSE Index

    Returns:
     Dict mapping index names to their data:
            - id: Index ID
            - auditId: (optional) Audit ID
            - exchangeIndexId: (optional) Exchange Index ID
            - generatedTime: (optional) Time the index was generated
            - index: Name of the index
            - close: Closing value
            - high: Highest value
            - low: Lowest value
            - previousClose: Previous closing value
            - change: Absolute change in index value
            - perChange: Percentage change in index value
            - fiftyTwoWeekHigh: 52-week high value
            - fiftyTwoWeekLow: 52-week low value
            - currentValue: Current value of the index
    Use this tool to get the latest NEPSE index and related index values. """
    try:
        response = fetch_nepse_api("/NepseIndex")
        validated = validate_and_return(response, NepseIndex)
        return validated.__root__ if hasattr(validated, "__root__") else response
    except Exception as e:
        logger.error(f"Error fetching NEPSE index: {e}")
        return {"error": str(e)}


def _get_index_graph(endpoint: str,limit: Optional[int] = None, page: Optional[int] = 1) -> dict:
    """
    Helper to fetch and paginate index graph data from NEPSE API.
    Returns paginated time series data.
    """
    try:
        raw_data = fetch_nepse_api(endpoint)
        # The API returns a list of [timestamp, value] pairs
        parsed = TimeSeriesData.from_list(raw_data)
        items = [tv.model_dump() if hasattr(tv, 'model_dump') else tv for tv in parsed.data]
        paged_items, total, page, limit = paginate_list(items, limit, page)
        return {
            "results": paged_items,
            "total": total,
            "page": page,
            "limit": limit
        }
    except Exception as e:
        logger.error(f"Error fetching {endpoint}: {e}")
        return {"error": str(e)}

@mcp.tool()
def get_daily_nepse_index_graph(limit: Optional[int] = None, page: Optional[int] = 1) -> dict:
    """
    Get daily NEPSE index graph (time series data). Supports pagination.
    Returns paginated list of {timestamp, value}.
    """
    return _get_index_graph("/DailyNepseIndexGraph", limit, page)

@mcp.tool()
def get_daily_sensitive_index_graph(limit: Optional[int] = None, page: Optional[int] = 1) -> dict:
    """
    Get daily Sensitive index graph (time series data). Supports pagination.
    Returns paginated list of {timestamp, value}.
    """
    return _get_index_graph("/DailySensitiveIndexGraph", limit, page)

@mcp.tool()
def get_daily_float_index_graph(limit: Optional[int] = None, page: Optional[int] = 1) -> dict:
    """
    Get daily Float index graph (time series data). Supports pagination.
    Returns paginated list of {timestamp, value}.
    """
    return _get_index_graph("/DailyFloatIndexGraph", limit, page)

@mcp.tool()
def get_daily_sensitive_float_index_graph(limit: Optional[int] = None, page: Optional[int] = 1) -> dict:
    """
    Get daily Sensitive Float index graph (time series data). Supports pagination.
    Returns paginated list of {timestamp, value}.
    """
    return _get_index_graph("/DailySensitiveFloatIndexGraph", limit, page)

@mcp.tool()
def get_daily_bank_subindex_graph(limit: Optional[int] = None, page: Optional[int] = 1) -> dict:
    """
    Get daily Bank subindex graph (time series data). Supports pagination.
    Returns paginated list of {timestamp, value}.
    """
    return _get_index_graph("/DailyBankSubindexGraph", limit, page)

@mcp.tool()
def get_daily_development_bank_subindex_graph(limit: Optional[int] = None, page: Optional[int] = 1) -> dict:
    """
    Get daily Development Bank subindex graph (time series data). Supports pagination.
    Returns paginated list of {timestamp, value}.
    """
    return _get_index_graph("/DailyDevelopmentBankSubindexGraph", limit, page)

@mcp.tool()
def get_daily_finance_subindex_graph(limit: Optional[int] = None, page: Optional[int] = 1) -> dict:
    """
    Get daily Finance subindex graph (time series data). Supports pagination.
    Returns paginated list of {timestamp, value}.
    """
    return _get_index_graph("/DailyFinanceSubindexGraph", limit, page)

@mcp.tool()
def get_daily_hotel_tourism_subindex_graph(limit: Optional[int] = None, page: Optional[int] = 1) -> dict:
    """
    Get daily Hotel & Tourism subindex graph (time series data). Supports pagination.
    Returns paginated list of {timestamp, value}.
    """
    return _get_index_graph("/DailyHotelTourismSubindexGraph", limit, page)

@mcp.tool()
def get_daily_hydropower_subindex_graph(limit: Optional[int] = None, page: Optional[int] = 1) -> dict:
    """
    Get daily Hydropower subindex graph (time series data). Supports pagination.
    Returns paginated list of {timestamp, value}.
    """
    return _get_index_graph("/DailyHydroPowerSubindexGraph", limit, page)

@mcp.tool()
def get_daily_investment_subindex_graph(limit: Optional[int] = None, page: Optional[int] = 1) -> dict:
    """
    Get daily Investment subindex graph (time series data). Supports pagination.
    Returns paginated list of {timestamp, value}.
    """
    return _get_index_graph("/DailyInvestmentSubindexGraph", limit, page)

@mcp.tool()
def get_daily_life_insurance_subindex_graph(limit: Optional[int] = None, page: Optional[int] = 1) -> dict:
    """
    Get daily Life Insurance subindex graph (time series data). Supports pagination.
    Returns paginated list of {timestamp, value}.
    """
    return _get_index_graph("/DailyLifeInsuranceSubindexGraph", limit, page)

@mcp.tool()
def get_daily_manufacturing_processing_subindex_graph(limit: Optional[int] = None, page: Optional[int] = 1) -> dict:
    """
    Get daily Manufacturing & Processing subindex graph (time series data). Supports pagination.
    Returns paginated list of {timestamp, value}.
    """
    return _get_index_graph("/DailyManufacturingProcessingSubindexGraph", limit, page)

@mcp.tool()
def get_daily_microfinance_subindex_graph(limit: Optional[int] = None, page: Optional[int] = 1) -> dict:
    """
    Get daily Microfinance subindex graph (time series data). Supports pagination.
    Returns paginated list of {timestamp, value}.
    """
    return _get_index_graph("/DailyMicrofinanceSubindexGraph", limit, page)

@mcp.tool()
def get_daily_mutual_fund_subindex_graph(limit: Optional[int] = None, page: Optional[int] = 1) -> dict:
    """
    Get daily Mutual Fund subindex graph (time series data). Supports pagination.
    Returns paginated list of {timestamp, value}.
    """
    return _get_index_graph("/DailyMutualFundSubindexGraph", limit, page)

@mcp.tool()
def get_daily_non_life_insurance_subindex_graph(limit: Optional[int] = None, page: Optional[int] = 1) -> dict:
    """
    Get daily Non-Life Insurance subindex graph (time series data). Supports pagination.
    Returns paginated list of {timestamp, value}.
    """
    return _get_index_graph("/DailyNonLifeInsuranceSubindexGraph", limit, page)

@mcp.tool()
def get_daily_others_subindex_graph(limit: Optional[int] = None, page: Optional[int] = 1) -> dict:
    """
    Get daily Others subindex graph (time series data). Supports pagination.
    Returns paginated list of {timestamp, value}.
    """
    return _get_index_graph("/DailyOthersSubindexGraph", limit, page)

@mcp.tool()
def get_daily_trading_subindex_graph(limit: Optional[int] = None, page: Optional[int] = 1) -> dict:
    """
    Get daily Trading subindex graph (time series data). Supports pagination.
    Returns paginated list of {timestamp, value}.
    """
    return _get_index_graph("/DailyTradingSubindexGraph", limit, page)

@mcp.tool()
def get_live_market(limit: Optional[int] = None, page: Optional[int] = 1) -> Dict:
    """
    Get real-time live market data for all securities with pagination support.
    Returns:
        Dict with:
            - results: List of securities, each with fields:
                - securityId, securityName, symbol, indexId, openPrice, highPrice, lowPrice,
                  totalTradeQuantity, totalTradeValue, lastTradedPrice, percentageChange,
                  lastUpdatedDateTime, lastTradedVolume, previousClose, averageTradedPrice,
                  totalTradedVolume (optional), numberOfTrades (optional)
            - total: Total number of securities
            - page: Current page number
            - limit: Number of results per page
    Use this tool to monitor live prices and volumes for all stocks when the market is open.
    """
    try:
        # Check if market is open first
        if not check_market_open():
            return {"error": "Market is closed. This tool only works when the market is open."}

        response = fetch_nepse_api("/LiveMarket")
        validated_data = validate_and_return(response, LiveMarketItem, is_list=True)
        items = [item.model_dump() if hasattr(item, 'model_dump') else item for item in validated_data]
        paged_items, total, page, limit = paginate_list(items, limit, page)
        return {
            "results": paged_items,
            "total": total,
            "page": page,
            "limit": limit
        }
    except Exception as e:
        logger.error(f"Error fetching live market: {e}")
        return {"error": str(e)}

def paginate_list(items, limit: Optional[int], page: Optional[int]):
    """Paginate a list of items. Returns (paged_items, total, page, limit)."""
    total = len(items)
    if limit is None or not isinstance(limit, int) or limit <= 0:
        limit = 10
    if page is None or not isinstance(page, int) or page <= 0:
        page = 1
    start = (page - 1) * limit
    end = start + limit
    paged_items = items[start:end]
    return paged_items, total, page, limit

@mcp.tool()
def get_price_volume(company: str = "", limit: Optional[int] = None, page: Optional[int] = 1) -> Dict:
    """
    Get price and volume data for all stocks, or filter by company name or symbol. Supports pagination.
    Args:
        company: (optional) Company name or symbol to filter (case-insensitive, partial match allowed). Leave empty for all stocks.
        limit: (optional) Number of results per page (default: 10).
        page: (optional) Page number for pagination (default: 1).
    Returns:
        Dict with:
            - results: List of stocks, each with fields:
                - securityId, securityName, symbol, indexId, totalTradeQuantity, lastTradedPrice,
                  percentageChange, previousClose, closePrice (optional)
            - total: Total number of stocks matching the filter
            - page: Current page number
            - limit: Number of results per page
    Use this tool to get price/volume for all stocks or search by company/symbol.
    """
    try:
        # Handle MCP framework parameter issues - convert various empty/null representations to None
        if company in ('None', 'null', '', 'undefined', None):
            company = None

        response = fetch_nepse_api("/PriceVolume")
        validated_data = validate_and_return(response, PriceVolumeItem, is_list=True)
        items = [item.model_dump() if hasattr(item, 'model_dump') else item for item in validated_data]

        # Filter by company if provided
        if company is not None and company.strip():
            company_clean = company.strip()
            logger.info(f"Filtering by company: '{company_clean}'")

            # Step 1: Check if it's a valid symbol first
            validation_result = validate_stock_symbol(company_clean)
            if validation_result.get("valid"):
                validated_symbol = validation_result["symbol"]
                logger.info(f"Found valid symbol: {validated_symbol}")

                filtered = [item for item in items if item.get("symbol", "").strip().upper() == validated_symbol.upper()]
                if filtered:
                    items = filtered
                    logger.info(f"Filtered to {len(items)} items by symbol")
                else:
                    return {"error": f"No price/volume data found for symbol '{validated_symbol}'."}
            else:
                # Step 2: Try to find symbol by company name
                logger.info(f"Not a valid symbol, trying company name lookup")
                symbol_lookup = find_symbol_by_company_name(company_clean)

                if symbol_lookup.get("found"):
                    matching_symbols = [match["symbol"] for match in symbol_lookup.get("matches", [])]
                    logger.info(f"Found symbols from company name lookup: {matching_symbols}")

                    filtered = [item for item in items if item.get("symbol", "").strip().upper() in [s.upper() for s in matching_symbols]]
                    if filtered:
                        items = filtered
                        logger.info(f"Filtered to {len(items)} items by company name")
                    else:
                        return {"error": f"No price/volume data found for company '{company_clean}'."}
                else:
                    # Step 3: No match found, return all companies with pagination
                    logger.info(f"No exact match found for '{company_clean}', returning all companies with pagination")
        else:
            logger.info("No company filter provided, returning paginated results")

        paged_items, total, page, limit = paginate_list(items, limit, page)
        logger.info(f"Returning {len(paged_items)} items out of {total} total")

        return {
            "results": paged_items,
            "total": total,
            "page": page,
            "limit": limit
        }
    except Exception as e:
        logger.error(f"Error fetching price volume: {e}")
        return {"error": str(e)}

@mcp.tool()
def get_top_gainers(limit: Optional[int] = None, page: Optional[int] = 1) -> Dict:
    """
    Get list of top gaining stocks with pagination support.
    Returns:
        Dict with:
            - results: List of stocks, each with fields:
                - symbol, ltp (last traded price), pointChange, percentageChange, securityName, securityId
            - total: Total number of top gainers
            - page: Current page number
            - limit: Number of results per page
    Use this tool to find which stocks have gained the most (by percentage) today.
    """
    try:
        response = fetch_nepse_api("/TopGainers")
        validated_data = validate_and_return(response, TopGainerLoser, is_list=True)
        items = [item.model_dump() if hasattr(item, 'model_dump') else item for item in validated_data]
        paged_items, total, page, limit = paginate_list(items, limit, page)
        return {
            "results": paged_items,
            "total": total,
            "page": page,
            "limit": limit
        }
    except Exception as e:
        logger.error(f"Error fetching top gainers: {e}")
        return {"error": str(e)}

@mcp.tool()
def get_top_losers(limit: Optional[int] = None, page: Optional[int] = 1) -> Dict:
    """
    Get list of top losing stocks with pagination support.
    Returns:
        Dict with:
            - results: List of stocks, each with fields:
                - symbol, ltp, pointChange, percentageChange, securityName, securityId
            - total: Total number of top losers
            - page: Current page number
            - limit: Number of results per page
    Use this tool to find which stocks have lost the most (by percentage) today.
    """
    try:
        response = fetch_nepse_api("/TopLosers")
        validated_data = validate_and_return(response, TopGainerLoser, is_list=True)
        items = [item.model_dump() if hasattr(item, 'model_dump') else item for item in validated_data]
        paged_items, total, page, limit = paginate_list(items, limit, page)
        return {
            "results": paged_items,
            "total": total,
            "page": page,
            "limit": limit
        }
    except Exception as e:
        logger.error(f"Error fetching top losers: {e}")
        return {"error": str(e)}

@mcp.tool()
def get_company_list(limit: Optional[int] = None, page: Optional[int] = 1) -> Dict:
    """
    Get list of all companies listed in NEPSE with pagination support.
    Returns:
        Dict with:
            - results: List of companies, each with fields:
                - id, companyName, symbol, securityName, status, companyEmail, website, sectorName,
                  regulatoryBody, instrumentType
            - total: Total number of companies
            - page: Current page number
            - limit: Number of results per page
    Use this tool to browse or search all listed companies.
    """
    try:
        response = fetch_nepse_api("/CompanyList")
        validated_data = validate_and_return(response, CompanyInfo, is_list=True)
        items = [item.model_dump() if hasattr(item, 'model_dump') else item for item in validated_data]
        paged_items, total, page, limit = paginate_list(items, limit, page)
        return {
            "results": paged_items,
            "total": total,
            "page": page,
            "limit": limit
        }
    except Exception as e:
        logger.error(f"Error fetching company list: {e}")
        return {"error": str(e)}

@mcp.tool()
def get_top_turnover(limit: Optional[int] = None, page: Optional[int] = 1) -> Dict:
    """
    Get top companies by turnover with pagination support.
    Returns:
        Dict with:
            - results: List of companies, each with fields:
                - symbol, turnover, closingPrice, securityName, securityId
            - total: Total number of top turnover companies
            - page: Current page number
            - limit: Number of results per page
    Use this tool to find companies with the highest trading turnover today.
    """
    try:
        response = fetch_nepse_api("/TopTenTurnoverScrips")
        validated_data = validate_and_return(response, TopTurnover, is_list=True)
        items = [item.model_dump() if hasattr(item, 'model_dump') else item for item in validated_data]
        paged_items, total, page, limit = paginate_list(items, limit, page)
        return {
            "results": paged_items,
            "total": total,
            "page": page,
            "limit": limit
        }
    except Exception as e:
        logger.error(f"Error fetching top turnover: {e}")
        return {"error": str(e)}

@mcp.tool()
def get_top_traders(limit: Optional[int] = None, page: Optional[int] = 1) -> Dict:
    """
    Get top traders by volume of Nepse securities with pagination support.
    Returns:
        Dict with:
            - results: List of securities, each with fields:
                - securityId, totalTrades, lastTradedPrice, securityName, symbol
            - total: Total number of top traders
            - page: Current page number
            - limit: Number of results per page
    Use this tool to find which securities had the most trades today.
    """
    try:
        response = fetch_nepse_api("/TopTenTradeScrips")
        validated_data = validate_and_return(response, TopTraders, is_list=True)
        items = [item.model_dump() if hasattr(item, 'model_dump') else item for item in validated_data]
        paged_items, total, page, limit = paginate_list(items, limit, page)
        return {
            "results": paged_items,
            "total": total,
            "page": page,
            "limit": limit
        }
    except Exception as e:
        logger.error(f"Error fetching top traders: {e}")
        return {"error": str(e)}

@mcp.tool()
def get_top_transactions(limit: Optional[int] = None, page: Optional[int] = 1) -> Dict:
    """
    Get top transactions by value for Nepse securities with pagination support.
    Returns:
        Dict with:
            - results: List of securities, each with fields:
                - securityId, totalTrades, lastTradedPrice, securityName, symbol
            - total: Total number of top transactions
            - page: Current page number
            - limit: Number of results per page
    Use this tool to find which securities had the highest transaction values today.
    """
    try:
        response = fetch_nepse_api("/TopTenTransactionScrips")
        validated_data = validate_and_return(response, TopTransactions, is_list=True)
        items = [item.model_dump() if hasattr(item, 'model_dump') else item for item in validated_data]
        paged_items, total, page, limit = paginate_list(items, limit, page)
        return {
            "results": paged_items,
            "total": total,
            "page": page,
            "limit": limit
        }
    except Exception as e:
        logger.error(f"Error fetching top transactions: {e}")
        return {"error": str(e)}

@mcp.tool()
def get_floorsheet(limit: Optional[int] = None, page: Optional[int] = 1) -> Dict:
    """
    Get today's floorsheet data (all transactions) with pagination support.
    Returns:
        Dict with:
            - results: List of trade contracts, each with fields:
                - contractId, stockSymbol, buyerMemberId, sellerMemberId, contractQuantity, contractRate,
                  contractAmount, businessDate, tradeBookId, stockId, buyerBrokerName, sellerBrokerName,
                  tradeTime, securityName
            - total: Total number of trades
            - page: Current page number
            - limit: Number of results per page
    Use this tool to explore all trades executed today after market close.
    """
    try:
        if check_market_open():
            return {"error": "Market is open. This tool works when the market is closed, it does not work when the market is open."}
        response = fetch_nepse_api("/Floorsheet")
        validated_data = validate_and_return(response, TradeContract, is_list=True)
        items = [item.model_dump() if hasattr(item, 'model_dump') else item for item in validated_data]
        paged_items, total, page, limit = paginate_list(items, limit, page)
        return {
            "results": paged_items,
            "total": total,
            "page": page,
            "limit": limit
        }
    except Exception as e:
        logger.error(f"Error fetching floorsheet: {e}")
        return {"error": str(e)}

@mcp.tool()
def get_company_floorsheet(symbol: str, limit: Optional[int] = None, page: Optional[int] = 1) -> Dict:
    """
    Get floorsheet data for a specific company with pagination support.
    Args:
        symbol: Stock symbol to filter trades.
        limit: (optional) Number of results per page (default: 10).
        page: (optional) Page number for pagination (default: 1).
    Returns:
        Dict with:
            - results: List of trade contracts for the company (see get_floorsheet for fields)
            - total: Total number of trades for the company
            - page: Current page number
            - limit: Number of results per page
            - symbol: The validated stock symbol
    Use this tool to see all trades for a specific company after market close.
    """
    try:
        if check_market_open():
            return {"error": "Market is open. This tool works when the market is closed, it does not work when the market is open."}
        validation_result = validate_stock_symbol(symbol)
        if not validation_result["valid"]:
            return {"error": validation_result["error"]}
        validated_symbol = validation_result["symbol"]
        response = fetch_nepse_api(f"/FloorsheetOf?symbol={validated_symbol}")
        validated_data = validate_and_return(response, TradeContract, is_list=True)
        items = [item.model_dump() if hasattr(item, 'model_dump') else item for item in validated_data]
        paged_items, total, page, limit = paginate_list(items, limit, page)
        return {
            "results": paged_items,
            "total": total,
            "page": page,
            "limit": limit,
            "symbol": validated_symbol
        }
    except Exception as e:
        logger.error(f"Error fetching company floorsheet for {symbol}: {e}")
        return {"error": str(e)}

@mcp.tool()
def get_price_history(symbol: str, limit: Optional[int] = None, page: Optional[int] = 1) -> Dict:
    """
    Get historical price and volume data for a company with pagination support.
    Args:
        symbol: Stock symbol to get history for.
        limit: (optional) Number of results per page (default: 10).
        page: (optional) Page number for pagination (default: 1).
    Returns:
        Dict with:
            - results: List of historical trade entries, each with fields:
                - businessDate, totalTrades, totalTradedQuantity, totalTradedValue, highPrice, lowPrice, closePrice
            - total: Total number of historical entries
            - page: Current page number
            - limit: Number of results per page
            - symbol: The validated stock symbol
    Use this tool to analyze a company's price and volume history.
    """
    try:
        validation_result = validate_stock_symbol(symbol)
        if not validation_result["valid"]:
            return {"error": validation_result["error"]}
        validated_symbol = validation_result["symbol"]
        response = fetch_nepse_api(f"/PriceVolumeHistory?symbol={validated_symbol}")
        validated_data = validate_and_return(response, HistoricalTradeEntry, is_list=True)
        items = [item.model_dump() if hasattr(item, 'model_dump') else item for item in validated_data]
        paged_items, total, page, limit = paginate_list(items, limit, page)
        return {
            "results": paged_items,
            "total": total,
            "page": page,
            "limit": limit,
            "symbol": validated_symbol
        }
    except Exception as e:
        logger.error(f"Error fetching price history for {symbol}: {e}")
        return {"error": str(e)}

@mcp.tool()
def get_market_depth(symbol: str) -> Dict:
    """
    Get market depth (bid/ask) for a specific stock.
    Args:
        symbol: Stock symbol to get market depth for.
    Returns:
        Dict with:
            - symbol: The stock symbol
            - totalBuyQty: Total buy quantity
            - totalSellQty: Total sell quantity
            - marketDepth: Object with buyMarketDepthList and sellMarketDepthList (each a list of MarketDepthItem)
            - timeStamp: (optional) Timestamp of the data
    Use this tool to analyze the current bid/ask depth for a stock when the market is open.
    """
    try:
        # Check if market is open first
        if not check_market_open():
            return {"error": "Market is closed. This tool only works when the market is open."}

        # Validate symbol
        validation_result = validate_stock_symbol(symbol)
        if not validation_result["valid"]:
            return {"error": validation_result["error"]}

        validated_symbol = validation_result["symbol"]
        response = fetch_nepse_api(f"/MarketDepth?symbol={validated_symbol}")
        validated_data = validate_and_return(response, MarketDepthResponse)
        return validated_data.model_dump() if hasattr(validated_data, 'model_dump') else response
    except Exception as e:
        logger.error(f"Error fetching market depth for {symbol}: {e}")
        return {"error": str(e)}

@mcp.tool()
def get_supply_demand(limit: Optional[int] = None, page: Optional[int] = 1) -> Dict:
    """
    Get the current supply and demand data for the NEPSE market, with pagination support.
    Args:
        limit: (optional) Number of results per page for both supply and demand lists (default: 10).
        page: (optional) Page number for pagination (default: 1).
    Returns:
        Dict with:
            - supplyList: Paginated list of supply (sell) orders, each with fields:
                - symbol, securityName, totalQuantity, totalOrder, securityId
            - demandList: Paginated list of demand (buy) orders, each with fields:
                - symbol, securityName, totalQuantity, totalOrder, securityId
            - totalSupply: Total number of supply entries before pagination
            - totalDemand: Total number of demand entries before pagination
            - page: Current page number
            - limit: Number of results per page
    Use this tool to analyze the current supply and demand (order book) for all stocks. Pagination is applied independently to both lists.
    """
    try:
        supply_demand_response = fetch_nepse_api("/SupplyDemand")
        # Use the server response directly as you provided
        supply_items = supply_demand_response["supplyList"]
        demand_items = supply_demand_response["demandList"]
        # Paginate both lists independently
        paged_supply, total_supply, page, limit = paginate_list(supply_items, limit, page)
        paged_demand, total_demand, _, _ = paginate_list(demand_items, limit, page)
        return {
            "supplyList": paged_supply,
            "demandList": paged_demand,
            "totalSupply": total_supply,
            "totalDemand": total_demand,
            "page": page,
            "limit": limit
        }
    except Exception as e:
        logger.error(f"Error fetching supply and demand data: {e}")
        return {"error": str(e)}

@mcp.tool()
def validate_stock_symbol_tool(symbol: str) -> Dict:
    """
    Validate if a stock symbol exists in NEPSE.
    Args:
        symbol: Stock symbol to validate.
    Returns:
        Dict with:
            - validation_result: Dict with keys 'valid', 'symbol', 'info' (if valid), 'error' (if invalid), and 'suggestions' (if invalid)
    Use this tool to check if a symbol is valid and get suggestions if not.
    """
    try:
        validation_result = validate_stock_symbol(symbol)
        return {"validation_result": validation_result}
    except Exception as e:
        logger.error(f"Error validating stock symbol {symbol}: {e}")
        return {"error": str(e)}

@mcp.tool()
def get_company_symbol(company_name: str) -> Dict:
    """
    Find stock symbol by company name. Use the first significant word of the company name for best results.
    Args:
        company_name: Name of the company to search for.
    Returns:
        Dict with:
            - search_result: Dict with keys 'found', 'query', 'matches' (list of symbol/company_name/match_type), 'total_matches', or 'error'
    Use this tool to find the NEPSE symbol for a company by its name.
    """
    try:
        result = find_symbol_by_company_name(company_name)
        return {"search_result": result}
    except Exception as e:
        logger.error(f"Error finding symbol for company name {company_name}: {e}")
        return {"error": str(e)}

@mcp.tool()
def get_company_name_from_symbol(symbol: str) -> Dict:
    """
    Find company name by stock symbol.
    Args:
        symbol: Stock symbol to look up.
    Returns:
        Dict with:
            - search_result: Dict with keys 'found', 'symbol', 'company_name', 'full_info', or 'error'
    Use this tool to get the full company name and info for a given NEPSE symbol.
    """
    try:
        result = find_company_name_by_symbol(symbol)
        return {"search_result": result}
    except Exception as e:
        logger.error(f"Error finding company name for symbol {symbol}: {e}")
        return {"error": str(e)}

@mcp.custom_route("/health", methods=["GET"])
async def health_check(request: Request) -> PlainTextResponse:
    return PlainTextResponse("OK")

# Update the transport to 'stdio' for local testing, 'http' for production / remote
if __name__ == "__main__":
    # for local testing
    # mcp.run(transport="stdio")
    # for production / remote
    mcp.run(transport="http", host="0.0.0.0", port=PORT)
