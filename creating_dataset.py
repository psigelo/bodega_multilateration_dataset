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
    beacons_names = beacons_information.name.unique()
    beamers_names = beamers_information.name.unique()
    mysql_cn = MySQLdb.connect(host='localhost',
                               port=3306,
                               user=user,
                               passwd=password,
                               db='beacon')
    all_data = pd.DataFrame()
    print("starting to create dataset")
    for beamer in beamers_names:
        print("beamer: ", beamer)
        for beacon in beacons_names:
            sql = "select payload from beacondata WHERE mac ='" + beacon + "' and gateway='" +\
                  beamer + "' order by id desc limit " + str(amount_of_rows_per_beacon) + ";"
            all_data = all_data.append(pd.read_sql(sql, con=mysql_cn))
    print("dataset all data obtained, storing in ./result/all.csv")
    mysql_cn.close()
    if not os.path.exists('./result/'):
        os.makedirs('./result/')
    all_data.to_csv('./result/all.csv')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--user", type=str, help="mysql user", required=True)
    parser.add_argument("-p", "--password", type=str, help="mysql password", required=True)
    parser.add_argument("-a", "--amount_of_rows_per_beacon", type=str, help="mysql password", required=True)
    parser_args = parser.parse_args()
    main(parser_args.user, parser_args.password, parser_args.amount_of_rows_per_beacon)
