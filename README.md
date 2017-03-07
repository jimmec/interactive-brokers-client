# Interactive Brokers API
The IB API is a client side asynchronous API that lets users talk to a local IB
Trader Workstation process in order to control all aspects of their IB brokerage
account.

# What is this
This is a hacked up client that allows you to download batches of historical
data using your existing IB brokerage account. It is largely based on the sample
API client provided by Interactive Brokers.

# How to run
Follow instructions from IB to coonect to your local IB TWS process.
1. launch TWS and login; make sure setting allows you to connect via API
2. run the main class in ApiDemo.java
3. connect via the configured port as in your TWS settings.
4. use the historical data panel to download batches of historical data
   configured in the MarketDataPanel.java class.
   a. you need to set the bar size and history period etc. from the UI.
