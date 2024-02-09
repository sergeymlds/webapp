# Data receiving application

This project was created to receive data using http requests.
The data is divided into several blocks according to their purpose. Each data block goes thought special route using POST method. The data is stored in the Azure Blob Storage.

This application was implemented in a real project to obtain data from each store of a large regional retail company. 

Here is a list of data that comes from grocery stores:
* daily sales
* daily stocks 
* promo 
* price changes
* actual list of goods
* markdowns
* safety stocks

Here is a list of data that goes to grocery stores:
* predictions
* predicted safety stocks

Forecasts are divided by store_id, which means that a request must contain a store ID in order to receive forecasts for the specific store.