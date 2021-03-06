import argparse
import os

import pandas as pd
import MySQLdb


def get_all_know_data():
    xls_beacons = pd.ExcelFile('./beacons_positions.xlsx')
    beacons_information = pd.read_excel(xls_beacons)
    xls_beamers = pd.ExcelFile('./beamers_positions.xlsx')
    beamers_information = pd.read_excel(xls_beamers)
    return beacons_information, beamers_information


def main(user, password, amount_of_rows_per_beacon):
    beacons_information, beamers_information = get_all_know_data()
    # beacons_names = beacons_information.name.unique()
    
    beamers_names = beamers_information.name.unique()
    mysql_cn = MySQLdb.connect(host='localhost',
                               port=3306,
                               user=user,
                               passwd=password,
                               db='beacon')
    sql = 'select mac from tenant_beacon;'
    beacons_names = pd.read_sql(sql, con=mysql_cn).mac.unique()
    print(beacons_names)
    all_data = pd.DataFrame()
    print("starting to create dataset")

    for beacon in beacons_names:

        sql = 'select a.id as b_id, a.mac as mac, a.payload->>"$.type" as type, a.payload->>"$.timestamp" as ts, a.payload->>"$.rssi" as rssi , a.gateway,a.updated_at from beacondata a ' \
              'WHERE mac =\'' + beacon + "'  AND updated_at >= DATE_SUB(NOW(),INTERVAL 1 HOUR);"
        all_data = all_data.append(pd.read_sql(sql, con=mysql_cn))
    print("dataset all data obtained, storing in ./result/all.csv")
    mysql_cn.close()
    all_data['rssi'] = pd.to_numeric(all_data['rssi'])
    if not os.path.exists('./result/'):
        os.makedirs('./result/')
    all_data.to_csv('./result/all.csv')
    print(all_data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--user", type=str, help="mysql user", required=True)
    parser.add_argument("-p", "--password", type=str, help="mysql password", required=True)
    parser.add_argument("-a", "--amount_of_rows_per_beacon", type=str, help="mysql password", required=True)
    parser_args = parser.parse_args()
    main(parser_args.user, parser_args.password, parser_args.amount_of_rows_per_beacon)
