import time
from bs4 import BeautifulSoup
import requests
import sqlite3
import os

# 変数urlにURLを格納する
url = 'https://db.netkeiba.com/race/202306050811/'

response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

# 変数d_listに空のリストを作成する
d_list = []

rows = soup.find_all('tr')[1:]  # 最初の行はヘッダーなのでスキップ

for row in rows:
    columns = row.find_all('td')

    # 必要な情報がない場合はスキップ
    if len(columns) < 14:
        continue

    # 馬名
    horse_name = columns[3].get_text(strip=True)  # 修正

    # タイム
    race_time = columns[7].get_text(strip=True)  # 修正

    # 馬体重
    horse_weight = columns[14].get_text(strip=True)  # 修正

    # スクレイピングしたデータを辞書に追加
    scraped_data = {"horse_name": horse_name, "race_time": race_time, "horse_weight": horse_weight}
    d_list.append(scraped_data)

# ローカルでの保存先ディレクトリ
path = '../db/'

# DBファイル名
db_name = 'dspro_final.sqlite'

# ディレクトリの存在確認と作成
if not os.path.exists(path):
    os.makedirs(path)

# DBに接続する（指定したDBファイルが存在しない場合は、新規に作成される）
con = sqlite3.connect(path + db_name)

# SQLを実行するためのオブジェクトを取得
cur = con.cursor()

# テーブルを作成するSQL
sql_create_table_git_hw1 = 'CREATE TABLE IF NOT EXISTS git_hw1(horse_name TEXT, race_time TEXT, horse_weight TEXT);'
cur.execute(sql_create_table_git_hw1)

# データを挿入するSQL
sql_insert_many = "INSERT INTO git_hw1(horse_name, race_time, horse_weight) VALUES (?, ?, ?);"
cur.executemany(sql_insert_many, [(hw["horse_name"], hw["race_time"], hw["horse_weight"]) for hw in d_list])

# コミット処理（データ操作を反映させる）
con.commit()

# DBへの接続を閉じる
con.close()

# DBに再度接続する（確認のため）
con = sqlite3.connect(path + db_name)
cur = con.cursor()

# SQLを用意してデータを取得
sql_select = 'SELECT * FROM git_hw1;'
cur.execute(sql_select)

# 取得したデータを表示
for r in cur:
    print(r)

# DBへの接続を閉じる
con.close()
