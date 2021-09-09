import pandas as pd
import numpy as np


def create_new_cols(df):
    df['lenAKS'] = df.apply(lambda row: len(row.AKS.split(".")), axis=1)
    df = df[df['lenAKS'] != 4]
    df['fleetID'] = df.apply(lambda row: row.AKS.split(".")[0], axis=1)
    df['shipID'] = df.apply(lambda row: row.AKS.split(".")[1], axis=1)
    df['engineID'] = df.apply(lambda row: row.AKS.split(
        ".")[2] if len(row.AKS.split(".")) == 6 else None, axis=1)
    df['shipMeasurement'] = df.apply(lambda row: row.AKS.split(
        ".")[2] if len(row.AKS.split(".")) == 3 else None, axis=1)
    df['alertType'] = df.apply(lambda row: row.AKS.split(
        ".")[3] if len(row.AKS.split(".")) > 3 else None, axis=1)
    return df


def create_new_cols_alerts(df):
    df['lenAKS'] = df.apply(lambda row: len(row.AKS.split(".")), axis=1)
    df['fleetID'] = df.apply(lambda row: row.AKS.split(".")[0], axis=1)
    df['shipID'] = df.apply(lambda row: row.AKS.split(".")[1], axis=1)
    # ship measurement
    df['shipMeasurement'] = df.apply(lambda row: row.AKS.split(
        ".")[2] if len(row.AKS.split(".")) == 3 else None, axis=1)
    # alert
    df['alertEngineID'] = df.apply(lambda row: row.AKS.split(
        ".")[2] if len(row.AKS.split(".")) == 4 else None, axis=1)
    df['alertMeasurement'] = df.apply(lambda row: row.AKS.split(
        ".")[3] if len(row.AKS.split(".")) == 4 else None, axis=1)
    # engine measurement
    df['engineID'] = df.apply(lambda row: row.AKS.split(
        ".")[2] if len(row.AKS.split(".")) == 5 else None, axis=1)
    df['engineAlertType'] = df.apply(lambda row: row.AKS.split(
        ".")[3] if len(row.AKS.split(".")) == 5 else None, axis=1)
    df['engineMeasurement'] = df.apply(lambda row: row.AKS.split(
        ".")[4] if len(row.AKS.split(".")) == 5 else None, axis=1)
    # df = pd.to_datetime(
    #     df['Zeitstempel']).apply(lambda x: x.date())
    return df


def split_data(df, variant):
    var = 3 if variant == 'ship' else 6
    df = df[df['lenAKS'] == var]
    return df


def remove_nan(df):
    df = df.where((pd.notnull(df)), None)

    return df
