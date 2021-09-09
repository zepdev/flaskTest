import os
from dotenv import load_dotenv
from flask import Flask, jsonify, make_response
from flask_cors import CORS
import requests
import pandas as pd
import numpy as np
from dbmanager import select, select_assets, select_asset_types, select_asset_groups, select_reading_keys, select_reading_values, select_customer, select_messages
from dfmanipulations import create_new_cols, split_data, create_new_cols_alerts

# Load environmental variables
load_dotenv()
app = Flask(__name__)
CORS(app)


@app.route('/')
def api_data():
    # Get all data tables from database and return as dataframes
    database = f"mssql+pymssql://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_DATABASE')}"
    # Get ship data
    df = select(database)
    df = create_new_cols(df)
    # Split ship data into engine and ship measurements
    df_ship = split_data(df, "ship")
    df_engine = split_data(df, 'engine')
    # reshape ship data
    df_ship = df_ship[['Zeitstempel', 'Wert', 'AKS', 'Einheit',
                       "Messstelle", "shipID", "shipMeasurement"]]
    df_ship = pd.pivot_table(
        df_ship, index=['shipID', 'Zeitstempel'], columns='Messstelle', values='Wert')
    # Merge ship measurements to engine measurements
    engine_data = pd.merge(df_engine, df_ship,  how='left', left_on=[
        'Zeitstempel', 'shipID'], right_on=['Zeitstempel', 'shipID'])

    # Get alert data
    df_alerts = select_messages(database)
    df_alerts = create_new_cols_alerts(df_alerts)
    # Append Alert Messages by code
    alertCodes = pd.read_csv('./Desktop/codes.csv', sep=';', header=None,
                             names=['alertMeasurement', 'alertMessage', 'nothing'])
    alertCodes = pd.DataFrame(alertCodes)
    alertCodes.drop('nothing', axis=1, inplace=True)
    alertCodes['alertMeasurement'] = alertCodes['alertMeasurement'].astype(str)
    df_alerts = pd.merge(df_alerts, alertCodes,
                         on="alertMeasurement", how="left")
    df_alerts['alertEngineID'] = df_alerts['alertEngineID'].astype(str)
    df_alerts['Zeitstempel'] = pd.to_datetime(df_alerts['Zeitstempel'])

    # Merge Alert Data to Engine Data
    dataset = pd.merge(engine_data, df_alerts,  how='left', left_on=[
        'Zeitstempel', 'engineID'], right_on=['Zeitstempel', 'alertEngineID'])

    return jsonify(dataset.to_dict())


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


def api_call():
    url = f"https://platform.smapone.com/Backend/v1/Smaps/242d7360-803a-4ea9-9551-98eba4919b43/Data/accessToken={os.getenv('accessToken')}"
    response = requests.get(url).json()
    print(response)


if __name__ == '__main__':
    app.run(debug=True)


# df3 = df[df['lenAKS'] == 6]
# df['engineID'].unique()
# df['test'] = df.apply(lambda row: str(row.engineID)[0:3], axis=1)
# df['lenAKS'].value_counts()
# len(df["engineID"].unique())
# pd.options.display.max_rows = 4000
# count = df['alertMeasurement'].value_counts()
# count = pd.DataFrame(count)
# count.to_csv('alertCodes.csv')
# pd.options.display.max_columns = None
# display(df)
