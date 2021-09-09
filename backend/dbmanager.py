import os
import sqlalchemy as db
import pandas as pd


def create_connection(database):
    try:
        engine = db.create_engine(database)
        connect = engine.connect()
        return engine, connect
    except:
        print("An error occurred connecting to the database")

    return None


def select(database):
    engine, connect = create_connection(database)
    query = connect.execute("""
        select v.Zeitstempel, v.Wert, k.Messstelle, k.AKS, k.* from dbo.Werte_Tag v
        join dbo.PE_Basis_Konfig k
        on k.PEID = v.PEID
        WHERE k.AKS like 'VI.%'
            AND v.Zeitstempel between '20180101' and '20190101'
        """)
    result = query.fetchall()
    df = pd.DataFrame(result)
    df.columns = query.keys()
#    ['ZeitstempelUTC', 'Wert', 'Messstelle', 'AKS', 'PEID', 'Typ', 'AKS', 'Anlage', 'Messstelle', 'Einheit', 'MeldeschablonenID', 'OPCHost', 'ServerName', 'PhysAdresse', 'Ersteller', 'Meldungsparameter', 'Meldungsverzoegerung', 'WertrangierungAufPEID', 'Kommentar', 'OBISKennzahl_C', 'Datenerfassung', 'HDAServerName', 'HDAItemID', 'HDAServerHost', 'ArchivpollingZyklus', 'ArchivpollingZeitspanne', 'MeldesequenzBeiArchivwertimport', 'EntprellzeitEndeAlsZeitstempel', 'OBISKennzahl_A']

    # ['Time stampUTC', 'value', 'measuring point', 'AKS', 'PEID', 'type', 'AKS', 'plant', 'measuring point', 'unit', 'reporting template ID', 'OPCHost', ' ServerName, PhysAddress, Creator, Message Parameter, Message Delay, Value RankingOnPEID, Comment, OBISKennzahl_C, Data Acquisition, HDAServerName, HDAItemID, HDAServerHost, Archive PollingCycle, 'ArchivepollingTime Span', 'Message SequenceInArchive Value Import', 'Debounce Time EndOfTimeStamp', 'OBISKennzahl_A']

    df = df.loc[:, ~df.columns.duplicated()]
    df = df[['Zeitstempel', 'Wert', 'AKS', 'Einheit', "Messstelle"]]
    engine.dispose()

    return df


def select_messages(database):
    # ship names, second part of AKS
    engine, connect = create_connection(database)
    query = connect.execute("""
        SELECT p.Messstelle, p.ServerName, p.AKS, m.Meldungstext, m.Richtung, m.Zeitstempel
        FROM dbo.Meldearchiv m
        LEFT JOIN dbo.PE_Basis_Konfig p
            ON p.PEID = m.PEID
        WHERE p.AKS like 'VI.%'
            AND m.Zeitstempel between '20180101' and '20190101'
            AND p.Messstelle not in ('WATCHDOG')
        """)
    result = query.fetchall()
    df = pd.DataFrame(result)
    df.columns = query.keys()
    engine.dispose()

    return df


def select_asset_groups(database):
    # ship names, second part of AKS
    engine, connect = create_connection(database)
    query = connect.execute("""
        SELECT DISTINCT sub.ServerName, sub.Customer as Customer, LEFT(sub.AKS, CHARINDEX('.' , sub.AKS)-1) as AKS
        FROM
            (SELECT ServerName, SUBSTRING(AKS, 1,
            CHARINDEX('.', AKS)-1) as Customer,
            SUBSTRING(AKS, CHARINDEX('.', AKS)+1, len(AKS)) as AKS
            FROM dbo.PE_Basis_Konfig
            WHERE ServerName != 'NULL'
            AND Datenerfassung = 4.0
            AND AKS LIKE '%.%') as sub
        """)
    result = query.fetchall()
    df = pd.DataFrame(result)
    df.columns = query.keys()
    engine.dispose()

    return df


def select_asset_types(database):
    engine, connect = create_connection(database)
    query = connect.execute("""
        SELECT distinct Bezeichnung
        FROM dbo.PEGruppen
        WHERE Bezeichnung LIKE 'M[1-9]%'
        """)
    result = query.fetchall()
    df = pd.DataFrame(result)
    df.columns = query.keys()
    engine.dispose()

    return df


def select_assets(database):
    # engine name
    engine, connect = create_connection(database)
    query = connect.execute("""
        SELECT pg.Bezeichnung, pg.AKS, LEFT(pg.Bezeichnung, CHARINDEX('-', pg.Bezeichnung)-1) as Position
        FROM dbo.PEGruppen pg
        WHERE pg.Bezeichnung LIKE 'M[1-9]%'
        """)
    result = query.fetchall()
    df = pd.DataFrame(result)
    df.columns = query.keys()

    return df


def select_customer(database):
    # Show what ID in AKS are for customer name / can filter just for VI data
    engine, connect = create_connection(database)
    query = connect.execute("""
        SELECT SUBSTRING(AKS, CHARINDEX('.', AKS)+1, len(AKS)) as AKS, Bezeichnung
        FROM dbo.PEGruppen
        WHERE AKS LIKE '%FM%'
        """)
    result = query.fetchall()
    df = pd.DataFrame(result)
    df.columns = query.keys()

    return df


def select_reading_keys(database):
    engine, connect = create_connection(database)
    query = connect.execute("""
        SELECT DISTINCT p.Messstelle, p.Einheit
        FROM dbo.PE_Basis_Konfig p
        WHERE p.ServerName NOT LIKE '%FlowChief%'
        AND p.Einheit IS NOT NULL
        AND p.Messstelle IS NOT NULL
        """)
    result = query.fetchall()
    df = pd.DataFrame(result)
    df.columns = query.keys()
    engine.dispose()

    return df


def select_reading_values(database, startDate, endDate):
    engine, connect = create_connection(database)
    query = connect.execute("""
        SELECT p.Messstelle, p.Einheit, p.ServerName, p.AKS, w.Zeitstempel,
        CASE WHEN w.Wert IS NULL
        THEN
            (Select TOP 1 w2.Wert
            FROM dbo.Werte_1min w2
            WHERE w.PEID = w2.PEID
            AND w2.Zeitstempel <= w.Zeitstempel
            AND w2.Wert IS NOT NULL
            ORDER BY w2.Zeitstempel DESC
            )
        ElSE
            w.Wert
        END as Wert
        FROM dbo.Werte_1min w
        LEFT JOIN dbo.PE_Basis_Konfig p
        ON p.PEID = w.PEID
        WHERE w.Zeitstempel between 01/01/2017 and '01/01/2019'
        AND p.ServerName NOT LIKE 'None'
        AND (p.AKS LIKE '%.%.%.%.%.%' OR p.AKS LIKE '%.%.%'
        """)
    result = query.fetchall()
    df = pd.DataFrame(result)
    df.columns = query.keys()
    engine.dispose()

    return df
