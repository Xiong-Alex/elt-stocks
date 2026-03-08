# yfinance Data Exploration Guide

This document lists the major types of data available through the
`yfinance` Python library and ideas for analytics or ETL pipelines using
each dataset.

------------------------------------------------------------------------

# Overview

`yfinance` provides access to Yahoo Finance market data including:

-   Historical price data
-   Company fundamentals
-   Financial statements
-   Analyst recommendations
-   Options chains
-   Ownership data
-   Dividends and splits
-   Earnings data
-   ESG metrics (sometimes available)

This data can be used for ETL pipelines, financial analytics,
dashboards, or machine learning projects.

------------------------------------------------------------------------

# 1. Historical Market Data

Access using:

    ticker.history()

Fields typically include:

-   Open
-   High
-   Low
-   Close
-   Volume
-   Dividends
-   Stock Splits

Possible explorations:

-   Volatility analysis
-   Trend detection
-   Technical indicators (RSI, MACD, moving averages)
-   Volume spikes vs price movement
-   Market regime detection
-   Intraday vs daily return analysis

------------------------------------------------------------------------

# 2. Company Metadata (Ticker Info)

Access using:

    ticker.info

Example fields:

-   sector
-   industry
-   marketCap
-   beta
-   trailingPE
-   forwardPE
-   dividendYield
-   country
-   fullTimeEmployees

Possible explorations:

-   Sector comparison
-   Market cap distribution
-   Valuation metrics across industries
-   Beta vs volatility studies
-   Dividend yield ranking

------------------------------------------------------------------------

# 3. Income Statement

Access using:

    ticker.financials

Contains:

-   Total Revenue
-   Gross Profit
-   Operating Income
-   Net Income
-   Research & Development
-   Cost of Revenue

Possible explorations:

-   Revenue growth trends
-   Profit margin analysis
-   R&D spending vs stock performance
-   Profitability rankings

------------------------------------------------------------------------

# 4. Balance Sheet

Access using:

    ticker.balance_sheet

Fields include:

-   Total Assets
-   Total Liabilities
-   Cash
-   Total Debt
-   Shareholder Equity

Possible explorations:

-   Debt ratios
-   Liquidity analysis
-   Financial health scoring
-   Debt vs market valuation

------------------------------------------------------------------------

# 5. Cash Flow Statements

Access using:

    ticker.cashflow

Fields include:

-   Operating Cash Flow
-   Capital Expenditures
-   Free Cash Flow

Possible explorations:

-   Free cash flow growth
-   Capital investment trends
-   Cash generation vs stock performance

------------------------------------------------------------------------

# 6. Earnings Data

Access using:

    ticker.earnings
    ticker.earnings_dates

Contains:

-   yearly revenue
-   yearly earnings
-   scheduled earnings announcements

Possible explorations:

-   Price reaction to earnings
-   Earnings surprise analysis
-   Revenue growth patterns

------------------------------------------------------------------------

# 7. Analyst Recommendations

Access using:

    ticker.recommendations

Fields:

-   analyst firm
-   rating
-   upgrade/downgrade history

Possible explorations:

-   Analyst sentiment vs stock returns
-   Upgrade/downgrade impact
-   Recommendation consensus scoring

------------------------------------------------------------------------

# 8. Institutional Holders

Access using:

    ticker.institutional_holders

Fields:

-   major institutions
-   number of shares held

Possible explorations:

-   Institutional ownership concentration
-   Institutional buying patterns
-   Ownership changes over time

------------------------------------------------------------------------

# 9. Options Chain

Access using:

    ticker.options
    ticker.option_chain(date)

Data includes:

-   strike price
-   last price
-   bid / ask
-   volume
-   open interest
-   implied volatility

Possible explorations:

-   Put/Call ratio sentiment
-   Implied volatility analysis
-   Options activity spikes
-   Volatility forecasting

------------------------------------------------------------------------

# 10. Dividends

Access using:

    ticker.dividends

Fields:

-   dividend payout history

Possible explorations:

-   Dividend growth
-   Dividend yield strategies
-   Dividend payout stability

------------------------------------------------------------------------

# 11. Stock Splits

Access using:

    ticker.splits

Possible explorations:

-   Pre/post split performance
-   Split frequency across sectors

------------------------------------------------------------------------

# 12. ESG / Sustainability Metrics (when available)

Access using:

    ticker.sustainability

Fields may include:

-   environmental score
-   governance score
-   social score

Possible explorations:

-   ESG score vs stock performance
-   ESG trends by industry

------------------------------------------------------------------------

# Example ETL Pipeline Ideas

Example data architecture:

    Data Source (yfinance API)
            ↓
    Bronze Layer (raw ingestion)
        - price history
        - fundamentals
        - options
        - earnings
            ↓
    Silver Layer (cleaned / normalized)
        - normalized price data
        - standardized financial metrics
        - cleaned options datasets
            ↓
    Gold Layer (analytics)
        - volatility indicators
        - earnings reaction metrics
        - options sentiment indicators
        - sector valuation dashboards

------------------------------------------------------------------------

# Example Project Ideas

1.  Stock volatility analytics platform
2.  Earnings reaction prediction model
3.  Options market sentiment dashboard
4.  Sector valuation comparison dashboard
5.  Institutional ownership tracker
6.  Dividend growth portfolio analyzer
7.  Analyst sentiment scoring engine
8.  Options implied volatility monitor

------------------------------------------------------------------------

# Notes

`yfinance` is unofficial and depends on Yahoo Finance endpoints. It is
suitable for:

-   personal projects
-   research
-   ETL pipeline demonstrations
-   financial analytics prototypes

For production systems, paid market data providers are typically used.
