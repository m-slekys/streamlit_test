import streamlit as st
import pickle
import duckdb
import pandas as pd
import numpy as np

# Load pickle files
with open(r'pickle/XGB_reg.pkl', 'rb') as input_file:
    XGB_reg = pickle.load(input_file)
with open(r'pickle/log_reg.pkl', 'rb') as input_file:
    log_reg = pickle.load(input_file)
with open(r'pickle/make_it_linear.pkl', 'rb') as input_file:
    make_it_linear = pickle.load(input_file)
with open(r'pickle/preproc_pipe.pkl', 'rb') as input_file:
    preprocessor = pickle.load(input_file)

# Load queries.
file = open("queries/bureau.txt", "r")
bureau_query = file.readline()
file = open("queries/credit_card_balance.txt", "r")
credit_card_balance_query = file.readline()
file = open("queries/installments_payments.txt", "r")
installments_payments_query = file.readline()
file = open("queries/POS_CASH_balance.txt", "r")
POS_CASH_balance_query = file.readline()
file = open("queries/previous_application.txt", "r")
previous_application_query = file.readline()
file.close()

#Load main table
path = "csv/test2.csv"
main = duckdb.read_csv(path)

num = int(st.text_input("Enter ID", value = 135287))



# Get row from main table
querry = "SELECT * FROM main WHERE SK_ID_CURR =" + str(id)
st.write(querry)

main = duckdb.sql(querry).fetchdf()

# Read CSV files as DuckDB relations.
path = "E:/M3_capstone/bureau.csv"
bureau_csv = duckdb.read_csv(path)
path = "E:/M3_capstone/credit_card_balance.csv"
credit_card_balance_csv = duckdb.read_csv(path)
path = "E:/M3_capstone/installments_payments.csv"
installments_payments_csv = duckdb.read_csv(path)
path = "E:/M3_capstone/POS_CASH_balance.csv"
POS_CASH_balance_csv = duckdb.read_csv(path)
path = "E:/M3_capstone/previous_application.csv"
previous_application_csv = duckdb.read_csv(path)

querry = f"WITH bureau AS ({bureau_query}), \
credit_card_balance AS ({credit_card_balance_query}), \
installments_payments AS ({installments_payments_query}), \
POS_CASH_balance AS ({POS_CASH_balance_query}), \
previous_application AS ({previous_application_query}) \
SELECT * FROM main AS t LEFT JOIN bureau AS b on t.SK_ID_CURR = b.SK_ID_CURR \
LEFT JOIN credit_card_balance AS c on t.SK_ID_CURR = c.SK_ID_CURR \
LEFT JOIN installments_payments AS i on t.SK_ID_CURR = i.SK_ID_CURR \
LEFT JOIN POS_CASH_balance AS p on t.SK_ID_CURR = p.SK_ID_CURR \
LEFT JOIN previous_application AS pa on t.SK_ID_CURR = pa.SK_ID_CURR \
\
;"

df = duckdb.sql(querry).fetchdf()

st.write(df)
