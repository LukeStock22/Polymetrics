Project Title: PolyMetrics
Goals: 
Aggregating a large amount of useful data
Develop an analytics dashboard for viewing (and/or analyzing) general and granular market and user data
Perform (and/or provide tools to perform) analysis on data at a high level:
Are Polymarket traders profitable? How much are they losing/making?
How much money is being traded across Polymarket?
What separates a profitable and unprofitable trader on Polymarket?
Perform  (and/or provide tools to perform) analysis on data at a granular level:
How does a specific user trade on Polymarket?
What trades are they making on specific markets? What strategies are they using (if any?)
Challenge: Integrate both historical and live data for analytics dashboard
Dataset: 
We will collect data on available contracts and user data/activity, accessed via official API (Historical and Live) (https://docs.polymarket.com/quickstart/overview)
There are 10,000+ markets available to draw data from
There are 650,000+ users
Some users, especially market makers, have over 100,000 trades
We do not expect serious difficulty outside of potential rate limits and mapping the data until a useful and organized relational database
Example for get user position
REQUEST: curl --request GET \
 --url 'https://data-api.polymarket.com/positions?sizeThreshold=1&limit=100&sortBy=TOKENS&sortDirection=DESC'
	      -    RESPONSE


Anticipated Roadblocks: 
Potential rate limits
Historical (resolved) market data granularity may be limited:
Concern: Using the Polymarket CLOB api for 15min BTC market: 
curl --request GET \ --url 'https://clob.polymarket.com/prices-history?market=51723722485033197856851688244994295680580973913127736734752917966518187333752&startTs=1764590400&endTs=1764591292&fidelity=0.5'
We can only get minute data, i.e. 15 data points. For long tracking bets (like presidential race), this is plenty. But for 15min markets, where lots of market makers trade, this is insufficient. 
Solutions: 
Test Gamma endpoint for closed market data
Collect our own data, focus on real time data tracking
Infer prices from user trade history - we will have collected enough fills to track all historical markets since we can get the full time-stamped trade history of every polymarket user. 
Real time data ingestion in general
Schema drift/discrepancies in contracts e.g. binary vs multi-outcome
Users could in theory have multiple wallets
Markets can resolve late, resolve ambiguously, or be cancelled
