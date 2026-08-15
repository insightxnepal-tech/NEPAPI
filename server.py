from fastapi import FastAPI, HTTPException, Response, Request
from fastapi.responses import JSONResponse
from nepse import AsyncNepse
from financial_reports import build_symbol_report, fetch_latest_listed_reports
import logging
import time
from typing import Optional

# Import validation utilities
from validator import validate_stock_symbol, validate_index_name, validator

# Import rate limiting
from rate_limiter import check_rate_limit, get_rate_limit_headers, rate_limiter

app = FastAPI()

# Rate limiting middleware
@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    # Get client IP
    client_ip = request.client.host
    if hasattr(request, 'headers'):
        # Check for forwarded IP (useful when behind proxy)
        forwarded_for = request.headers.get('X-Forwarded-For')
        if forwarded_for:
            client_ip = forwarded_for.split(',')[0].strip()

    # Check rate limit
    endpoint = request.url.path
    allowed, info = check_rate_limit(client_ip, endpoint)

    if not allowed:
        headers = get_rate_limit_headers(info)
        retry_after = info["reset_time"] - int(time.time())
        headers["Retry-After"] = str(retry_after)

        return JSONResponse(
            status_code=429,
            content={
                "error": "Rate limit exceeded",
                "message": f"Too many requests. Try again in {retry_after} seconds.",
                "limit": info["limit"],
                "reset_time": info["reset_time"]
            },
            headers=headers
        )

    # Process request
    response = await call_next(request)

    # Add rate limit headers to response
    headers = get_rate_limit_headers(info)
    for key, value in headers.items():
        response.headers[key] = value

    return response

# Common validation helper functions
def validate_stock_or_raise(symbol: str) -> str:
    """Validate stock symbol and raise HTTPException if invalid"""
    validation_result = validate_stock_symbol(symbol)
    if not validation_result["valid"]:
        error_msg = validation_result["error"]
        if validation_result.get("suggestions"):
            error_msg += f" Suggestions: {', '.join(validation_result['suggestions'])}"
        raise HTTPException(status_code=400, detail=error_msg)
    return validation_result["symbol"]

def validate_index_or_raise(index_name: str) -> str:
    """Validate index name and raise HTTPException if invalid"""
    validation_result = validate_index_name(index_name)
    if not validation_result["valid"]:
        error_msg = validation_result["error"]
        if validation_result.get("available_indices"):
            error_msg += f" Available indices: {', '.join(validation_result['available_indices'])}"
        raise HTTPException(status_code=400, detail=error_msg)
    return validation_result["index_name"]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

#pip install --upgrade git+https://github.com/basic-bgnr/NepseUnofficialApi.git

#onrender - pip3 install --upgrade git+https://github.com/surajrimal07/NepseAPI.git@dev
nepseAsync = AsyncNepse()
nepseAsync.setTLSVerification(False)

routes = {
    "Health": "/health",
    "Docs": "/docs",
    "PriceVolume": "/PriceVolume",
    "Summary": "/Summary",
    "SupplyDemand": "/SupplyDemand",
    "TopGainers": "/TopGainers",
    "TopLosers": "/TopLosers",
    "TopTenTradeScrips": "/TopTenTradeScrips",
    "TopTenTurnoverScrips": "/TopTenTurnoverScrips",
    "TopTenTransactionScrips": "/TopTenTransactionScrips",
    "IsNepseOpen": "/IsNepseOpen",
    "NepseIndex": "/NepseIndex",
    "NepseSubIndices": "/NepseSubIndices",
    "DailyScripPriceGraph": "/DailyScripPriceGraph",
    "CompanyList": "/CompanyList",
    "SectorScrips": "/SectorScrips",
    "MarketDepth": "/MarketDepth",
    "CompanyDetails": "/CompanyDetails",
    "Floorsheet": "/Floorsheet",
    "FloorsheetOf": "/FloorsheetOf",
    "PriceVolumeHistory": "/PriceVolumeHistory",
    "SecurityList": "/SecurityList",
    "TradeTurnoverTransactionSubindices": "/TradeTurnoverTransactionSubindices",
    "LiveMarket": "/LiveMarket",
    "DailyNepseIndexGraph": "/DailyNepseIndexGraph",
    "DailySensitiveIndexGraph": "/DailySensitiveIndexGraph",
    "DailyFloatIndexGraph": "/DailyFloatIndexGraph",
    "DailySensitiveFloatIndexGraph": "/DailySensitiveFloatIndexGraph",
    "DailyBankSubindexGraph": "/DailyBankSubindexGraph",
    "DailyDevelopmentBankSubindexGraph": "/DailyDevelopmentBankSubindexGraph",
    "DailyFinanceSubindexGraph": "/DailyFinanceSubindexGraph",
    "DailyHotelTourismSubindexGraph": "/DailyHotelTourismSubindexGraph",
    "DailyHydroPowerSubindexGraph": "/DailyHydroPowerSubindexGraph",
    "DailyInvestmentSubindexGraph": "/DailyInvestmentSubindexGraph",
    "DailyLifeInsuranceSubindexGraph": "/DailyLifeInsuranceSubindexGraph",
    "DailyManufacturingProcessingSubindexGraph": "/DailyManufacturingProcessingSubindexGraph",
    "DailyMicrofinanceSubindexGraph": "/DailyMicrofinanceSubindexGraph",
    "DailyMutualFundSubindexGraph": "/DailyMutualFundSubindexGraph",
    "DailyNonLifeInsuranceSubindexGraph": "/DailyNonLifeInsuranceSubindexGraph",
    "DailyOthersSubindexGraph": "/DailyOthersSubindexGraph",
    "DailyTradingSubindexGraph": "/DailyTradingSubindexGraph",
    "FinancialReports": "/FinancialReports",
    "LatestFinancialReports": "/LatestFinancialReports",
}

HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Cache-Control": "public, max-age=30"
}

FINANCIAL_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Cache-Control": "public, max-age=3600"
}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.get("/rate-limit/stats")
async def get_rate_limit_stats():
    """Get rate limiting statistics"""
    stats = rate_limiter.get_stats()
    return JSONResponse(content=stats, headers=HEADERS)

@app.get("/validate/stock/{symbol}")
async def validate_stock(symbol: str):
    """Validate a stock symbol and return validation result"""
    validation_result = validate_stock_symbol(symbol)
    if validation_result["valid"]:
        return JSONResponse(content=validation_result, headers=HEADERS)
    else:
        return JSONResponse(
            content=validation_result,
            status_code=404,
            headers={"Access-Control-Allow-Origin": "*"}
        )

@app.get("/validate/index/{index_name}")
async def validate_index(index_name: str):
    """Validate an index name and return validation result"""
    validation_result = validate_index_name(index_name)
    if validation_result["valid"]:
        return JSONResponse(content=validation_result, headers=HEADERS)
    else:
        return JSONResponse(
            content=validation_result,
            status_code=404,
            headers={"Access-Control-Allow-Origin": "*"}
        )

@app.get("/validation/stats")
async def get_validation_stats():
    """Get validation statistics"""
    stats = validator.get_stats()
    return JSONResponse(content=stats, headers=HEADERS)

@app.get("/")
async def get_index():
    content = "<ul>" + "".join([f"<li><a href={value}>{key}</a></li>" for key, value in routes.items()]) + "</ul>"
    html_content = f"<h1>Serving hot stock data using FastAPI</h1>{content}"
    return Response(content=html_content, media_type="text/html")

@app.get(routes["Summary"])
async def get_summary():
    data = await _get_summary()
    return JSONResponse(content= data, headers=HEADERS)


async def _get_summary():
    response = dict()
    for obj in await nepseAsync.getSummary():
        response[obj["detail"]] = obj["value"]
    return response

@app.get(routes["NepseIndex"])
async def get_nepse_index():
    return JSONResponse(content= await _get_nepse_index(), headers=HEADERS)


async def _get_nepse_index():
    response = dict()
    for obj in await nepseAsync.getNepseIndex():
        response[obj["index"]] = obj
    return response

@app.get(routes["LiveMarket"])
async def get_live_market():
    data = await nepseAsync.getLiveMarket()
    return JSONResponse(content=data, headers=HEADERS)


#Bugged, hoping for fix from the library
@app.get(routes["MarketDepth"])
async def get_market_depth(symbol: str):
    validated_symbol = validate_stock_or_raise(symbol)
    data = await nepseAsync.getSymbolMarketDepth(validated_symbol)
    return JSONResponse(content=data, headers=HEADERS)

@app.get(routes["NepseSubIndices"])
async def get_nepse_subindices():
    data = await _get_nepse_subindices()
    return JSONResponse(content=data, headers=HEADERS)

async def _get_nepse_subindices():
    response = dict()
    for obj in await nepseAsync.getNepseSubIndices():
        response[obj["index"]] = obj
    return response

@app.get(routes["TopTenTradeScrips"])
async def get_top_ten_trade_scrips():
    data = await nepseAsync.getTopTenTradeScrips()
    return JSONResponse(content=data, headers=HEADERS)


@app.get(routes["TopTenTransactionScrips"])
async def get_top_ten_transaction_scrips():
    data = await nepseAsync.getTopTenTransactionScrips()
    return JSONResponse(content=data, headers=HEADERS)


@app.get(routes["TopTenTurnoverScrips"])
async def get_top_ten_turnover_scrips():
    data = await nepseAsync.getTopTenTurnoverScrips()
    return JSONResponse(content=data, headers=HEADERS)


@app.get(routes["SupplyDemand"])
async def get_supply_demand():
    data = await nepseAsync.getSupplyDemand()
    return JSONResponse(content=data, headers=HEADERS)


@app.get(routes["TopGainers"])
async def get_top_gainers():
    data = await nepseAsync.getTopGainers()
    return JSONResponse(content=data, headers=HEADERS)


@app.get(routes["TopLosers"])
async def get_top_losers():
    data = await nepseAsync.getTopLosers()
    return JSONResponse(content=data, headers=HEADERS)


@app.get(routes["IsNepseOpen"])
async def is_nepse_open():
    logger.info("IsNepseOpen endpoint called")
    data = await nepseAsync.isNepseOpen()
    return JSONResponse(content=data, headers=HEADERS)


@app.get(routes["DailyNepseIndexGraph"])
async def get_daily_nepse_index_graph():
    data = await nepseAsync.getDailyNepseIndexGraph()
    return JSONResponse(content=data, headers=HEADERS)

@app.get(routes["DailySensitiveIndexGraph"])
async def get_daily_sensitive_index_graph():
    data = await nepseAsync.getDailySensitiveIndexGraph()
    return JSONResponse(content=data, headers=HEADERS)

@app.get(routes["DailyFloatIndexGraph"])
async def get_daily_float_index_graph():
    data = await nepseAsync.getDailyFloatIndexGraph()
    return JSONResponse(content=data, headers=HEADERS)

@app.get(routes["DailySensitiveFloatIndexGraph"])
async def get_daily_sensitive_float_index_graph():
    data = await nepseAsync.getDailySensitiveFloatIndexGraph()
    return JSONResponse(content=data, headers=HEADERS)

@app.get(routes["DailyBankSubindexGraph"])
async def get_daily_bank_subindex_graph():
    data = await nepseAsync.getDailyBankSubindexGraph()
    return JSONResponse(content=data, headers=HEADERS)

@app.get(routes["DailyDevelopmentBankSubindexGraph"])
async def get_daily_development_bank_subindex_graph():
    data = await nepseAsync.getDailyDevelopmentBankSubindexGraph()
    return JSONResponse(content=data, headers=HEADERS)

@app.get(routes["DailyFinanceSubindexGraph"])
async def get_daily_finance_subindex_graph():
    data = await nepseAsync.getDailyFinanceSubindexGraph()
    return JSONResponse(content=data, headers=HEADERS)

@app.get(routes["DailyHotelTourismSubindexGraph"])
async def get_daily_hotel_tourism_subindex_graph():
    data = await nepseAsync.getDailyHotelTourismSubindexGraph()
    return JSONResponse(content=data, headers=HEADERS)

@app.get(routes["DailyHydroPowerSubindexGraph"])
async def get_daily_hydro_power_subindex_graph():
    data = await nepseAsync.getDailyHydroSubindexGraph()
    return JSONResponse(content=data, headers=HEADERS)


@app.get(routes["DailyInvestmentSubindexGraph"])
async def get_daily_investment_subindex_graph():
    data = await nepseAsync.getDailyInvestmentSubindexGraph()
    return JSONResponse(content=data, headers=HEADERS)

@app.get(routes["DailyLifeInsuranceSubindexGraph"])
async def get_daily_life_insurance_subindex_graph():
    data = await nepseAsync.getDailyLifeInsuranceSubindexGraph()
    return JSONResponse(content=data, headers=HEADERS)

@app.get(routes["DailyManufacturingProcessingSubindexGraph"])
async def get_daily_manufacturing_processing_subindex_graph():
    data = await nepseAsync.getDailyManufacturingSubindexGraph()
    return JSONResponse(content=data, headers=HEADERS)

@app.get(routes["DailyMicrofinanceSubindexGraph"])
async def get_daily_microfinance_subindex_graph():
    data = await nepseAsync.getDailyMicrofinanceSubindexGraph()
    return JSONResponse(content=data, headers=HEADERS)

@app.get(routes["DailyMutualFundSubindexGraph"])
async def get_daily_mutual_fund_subindex_graph():
    data = await nepseAsync.getDailyMutualfundSubindexGraph()
    return JSONResponse(content=data, headers=HEADERS)

@app.get(routes["DailyNonLifeInsuranceSubindexGraph"])
async def get_daily_non_life_insurance_subindex_graph():
    data = await nepseAsync.getDailyNonLifeInsuranceSubindexGraph()
    return JSONResponse(content=data, headers=HEADERS)

@app.get(routes["DailyOthersSubindexGraph"])
async def get_daily_others_subindex_graph():
    data = await nepseAsync.getDailyOthersSubindexGraph()
    return JSONResponse(content=data, headers=HEADERS)

@app.get(routes["DailyTradingSubindexGraph"])
async def get_daily_trading_subindex_graph():
    data = await nepseAsync.getDailyTradingSubindexGraph()
    return JSONResponse(content=data, headers=HEADERS)

@app.get(routes["DailyScripPriceGraph"])
async def get_daily_scrip_price_graph(symbol: str):
    validated_symbol = validate_stock_or_raise(symbol)
    data = await nepseAsync.getDailyScripPriceGraph(validated_symbol)
    return JSONResponse(content=data, headers=HEADERS)


@app.get(routes["CompanyList"])
async def get_company_list():
    data = await nepseAsync.getCompanyList()
    return JSONResponse(content=data, headers=HEADERS)


@app.get(routes["SectorScrips"])
async def get_sector_scrips():
    data = await nepseAsync.getSectorScrips()
    return JSONResponse(content=data, headers=HEADERS)


@app.get(routes["CompanyDetails"])
async def get_company_details(symbol: str):
    validated_symbol = validate_stock_or_raise(symbol)
    data = await nepseAsync.getCompanyDetails(validated_symbol)
    return JSONResponse(content=data, headers=HEADERS)


@app.get(routes["FinancialReports"])
async def get_financial_reports(symbol: str):
    """Latest and historical quarterly/annual reports for one listed stock."""
    validated_symbol = validate_stock_or_raise(symbol)
    data = await build_symbol_report(nepseAsync, validated_symbol, include_reports=True)
    return JSONResponse(content=data, headers=FINANCIAL_HEADERS)


@app.get(routes["LatestFinancialReports"])
async def get_latest_financial_reports(
    symbol: Optional[str] = None,
    sector: Optional[str] = None,
    force_refresh: bool = False,
):
    """
    Latest quarterly and annual report for listed equity stocks.

    Omit `symbol` to return every active equity listing (cached for 6 hours).
    """
    symbols = [validate_stock_or_raise(symbol)] if symbol else None
    data = await fetch_latest_listed_reports(
        nepseAsync,
        symbols=symbols,
        sector=sector,
        force_refresh=force_refresh,
    )
    return JSONResponse(content=data, headers=FINANCIAL_HEADERS)


@app.get(routes["PriceVolume"])
async def get_price_volume():
    data = await nepseAsync.getPriceVolume()
    return JSONResponse(content=data, headers=HEADERS)


@app.get(routes["PriceVolumeHistory"])
async def get_price_volume_history(symbol: str):
    validated_symbol = validate_stock_or_raise(symbol)
    data = await nepseAsync.getCompanyPriceVolumeHistory(validated_symbol)
    return JSONResponse(content=data, headers=HEADERS)


@app.get(routes["Floorsheet"])
async def get_floorsheet():
    floorsheet_data = await nepseAsync.getFloorSheet()
    return JSONResponse(content=floorsheet_data, headers=HEADERS)


@app.get(routes["FloorsheetOf"])
async def get_floorsheet_of(symbol: str):
    validated_symbol = validate_stock_or_raise(symbol)
    data = await nepseAsync.getFloorSheetOf(validated_symbol)
    return JSONResponse(content=data, headers=HEADERS)


@app.get(routes["SecurityList"])
async def getSecurityList():
    data = await nepseAsync.getSecurityList()
    return JSONResponse (content=data, headers=HEADERS)

@app.get(routes["TradeTurnoverTransactionSubindices"])
async def getTradeTurnoverTransactionSubindices():
    companies = {company["symbol"]: company for company in await nepseAsync.getCompanyList()}

    turnover = {obj["symbol"]: obj for obj in await nepseAsync.getTopTenTurnoverScrips()}
    transaction = {obj["symbol"]: obj for obj in await nepseAsync.getTopTenTransactionScrips()}
    trade = {obj["symbol"]: obj for obj in await nepseAsync.getTopTenTradeScrips()}

    gainers = {obj["symbol"]: obj for obj in await nepseAsync.getTopGainers()}
    losers = {obj["symbol"]: obj for obj in await nepseAsync.getTopLosers()}

    price_vol_info = {obj["symbol"]: obj for obj in await nepseAsync.getPriceVolume()}

    sector_sub_indices = await _getNepseSubIndices()
    # this is done since nepse sub indices and sector name are different
    sector_mapper = {
        "Commercial Banks": "Banking SubIndex",
        "Development Banks": "Development Bank Index",
        "Finance": "Finance Index",
        "Hotels And Tourism": "Hotels And Tourism Index",
        "Hydro Power": "HydroPower Index",
        "Investment": "Investment Index",
        "Life Insurance": "Life Insurance",
        "Manufacturing And Processing": "Manufacturing And Processing",
        "Microfinance": "Microfinance Index",
        "Mutual Fund": "Mutual Fund",
        "Non Life Insurance": "Non Life Insurance",
        "Others": "Others Index",
        "Tradings": "Trading Index",
    }

    scrips_details = {}
    for symbol, company in companies.items():
        company_details = {
            "symbol": symbol,
            "sector": company["sectorName"],
            "Turnover": turnover.get(symbol, {}).get("turnover", 0),
            "transaction": transaction.get(symbol, {}).get("totalTrades", 0),
            "volume": trade.get(symbol, {}).get("shareTraded", 0),
            "previousClose": price_vol_info.get(symbol, {}).get("previousClose", 0),
            "lastUpdatedDateTime": price_vol_info.get(symbol, {}).get("lastUpdatedDateTime", 0),
            "name": company.get("securityName", ""),
            "lastUpdatedDateTime": price_vol_info.get(symbol, {}).get("lastUpdatedDateTime", 0),
            "category": company.get("instrumentType"),
        }

        if symbol in gainers:
            company_details.update({
                "pointChange": gainers[symbol]["pointChange"],
                "percentageChange": gainers[symbol]["percentageChange"],
                "ltp": gainers[symbol]["ltp"],
            })
        elif symbol in losers:
            company_details.update({
                "pointChange": losers[symbol]["pointChange"],
                "percentageChange": losers[symbol]["percentageChange"],
                "ltp": losers[symbol]["ltp"],
            })
        else:
            company_details.update({
                "pointChange": 0,
                "percentageChange": 0,
                "ltp": 0,
            })

        #let's filter here based on ltp and previos close, if ltp = 0 or previous close = 0, then the company is not trading
        if company_details["ltp"] == 0 or company_details["previousClose"] == 0:
            continue
        scrips_details[symbol] = company_details

    sector_details = dict()
    sectors = {company["sectorName"] for company in companies.values()}
    for sector in sectors:
        total_trades, total_trade_quantity, total_turnover = 0, 0, 0
        for scrip_details in scrips_details.values():
            if scrip_details["sector"] == sector:
                total_trades += scrip_details["transaction"]
                total_trade_quantity += scrip_details["volume"]
                total_turnover += scrip_details["Turnover"]

        sector_details[sector] = {
            "transaction": total_trades,
            "volume": total_trade_quantity,
            "totalTurnover": total_turnover,
            "turnover": sector_sub_indices[sector_mapper[sector]],
            "sectorName": sector,
        }

    return JSONResponse({"scripsDetails": scrips_details, "sectorsDetails": sector_details}, headers=HEADERS)

async def _getNepseSubIndices():
    return {obj["index"]: obj for obj in await nepseAsync.getNepseSubIndices()}

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)