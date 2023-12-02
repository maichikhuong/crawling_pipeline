import duckdb
import streamlit as st

# initiate the MotherDuck connection through a service token through
con = duckdb.connect('md:?motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoibWFpY2hpa2h1b25nOTguZ21haWwuY29tIiwiZW1haWwiOiJtYWljaGlraHVvbmc5OEBnbWFpbC5jb20iLCJ1c2VySWQiOiI0NDdjZTk5NC05N2ZjLTQ4YmMtOTA1Mi1jMDE4ZmU0OGY1MTgiLCJpYXQiOjE3MDEyMzA0OTIsImV4cCI6MTczMjc4ODA5Mn0.2mwUo_yTq_ygchdVwjMY-Xe4zciDJejgttfo0MLFCO8')


df = con.sql(f"""
SELECT *
from my_db.main.hose
""").df()


st.write(df)



