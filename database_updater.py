import requests
from lxml import etree
import psycopg2
from datetime import datetime
import time
from joblib import Parallel, delayed

def get_files(reg, tmp_link, db_cursor, insert_qry, db, update_qry):
    headed = 0
    #        if count == 6:
    #            time.sleep(1)
    #            count = 1
    main_file = requests.get(reg[3])
    parsed = main_file.text
    for line in parsed.splitlines():
        if '|' in line and headed == 0:
            headed = 1
        elif '|' in line and headed == 1:
            line_splited = line.split('|')
            record_to_insert = (reg[0], int(line_splited[0]), line_splited[1], line_splited[2], line_splited[3],
                                f'{tmp_link}{line_splited[4]}', False)
            try:
                db_cursor.execute(insert_qry, record_to_insert)
                db.commit()
            except Exception as error:
                print(f'Error {error}')
                db.rollback()
    try:
        db_cursor.execute(update_qry, (1, reg[0]))
        db.commit()
    except Exception as error:
        print(error)

def master_idx_updater(db):
    qry_qtr_table_sorted = '''SELECT * FROM QTR_INDEX WHERE CRAWLED = 0 ORDER BY YEAR, QUARTER'''
    db_cursor = db.cursor()
    db_cursor.execute(qry_qtr_table_sorted)
    table_sorted = db_cursor.fetchall()
    db_cursor.close()
    insert_qry = '''INSERT INTO MASTER_IDX (qtr_id, cik, cname, form, date, link, downloaded) VALUES (%s, %s, %s, %s, %s, %s, %s) '''
    update_qry = '''UPDATE PUBLIC.QTR_INDEX SET CRAWLED = %s WHERE ID = %s;'''
    tmp_link = f'https://www.sec.gov/Archives/'
    count = 1
    db_cursor = db.cursor()
    Parallel(n_jobs=5, require="sharedmem")(delayed(get_files)(reg, tmp_link, db_cursor, insert_qry, db, update_qry) for reg in table_sorted)
#        headed = 0
#        if count == 6:
#            time.sleep(1)
#            count = 1
#        main_file = requests.get(reg[3])
#        parsed = main_file.text
#        for line in parsed.splitlines():
#            if '|' in line and headed == 0:
#                headed = 1
#            elif '|' in line and headed == 1:
#                line_splited = line.split('|')
#                record_to_insert = (reg[0], int(line_splited[0]), line_splited[1], line_splited[2], line_splited[3], f'{tmp_link}{line_splited[4]}', False)
#                try:
#                    db_cursor.execute(insert_qry, record_to_insert)
#                    db.commit()
#                except Exception as error:
#                    print(f'Error {error}')
#                    db.rollback()
#        try:
#            db_cursor.execute(update_qry, (1, reg[0]))
#            db.commit()
#        except Exception as error:
#            print(error)
#        count += 1


#    print(parsed)



def main():
    #    sys.exec
    #    server = machine.Machine
    db = psycopg2.connect(user='postgres',
                          password='postgres',
                          host='0.0.0.0',
                          port='5432',
                          database='sec-gov')
    cur_year = datetime.now().year
    cur_month = datetime.now().month
    if cur_month < 4:
        cur_qtr = 1
    elif cur_month < 7:
        cur_qtr = 2
    elif cur_month < 10:
        cur_qtr = 3
    else:
        cur_qtr = 4
    db_cursor = db.cursor()
    insert_qry = '''INSERT INTO QTR_INDEX (YEAR, QUARTER, URL, CRAWLED) VALUES (%s, %s, %s, %s)'''
    index = 1
    xmlDict = {}
    qry_qtr_table_sorted = '''SELECT * FROM QTR_INDEX ORDER BY YEAR, QUARTER'''
    db_cursor.execute(qry_qtr_table_sorted)
    table_sorted = db_cursor.fetchall()
    if len(table_sorted) == 0:
        url = 'https://www.sec.gov/Archives/edgar/full-index/sitemap.quarterlyindexes.xml'
        main_file = requests.get(url)
        parsed = etree.fromstring(main_file.content)
        print("The number of sitemap tags are {0}".format(len(parsed)))
        for sitemap in parsed:
            child = sitemap.getchildren()
            xmlDict[index] = child[1].text
            list_splited = child[1].text.split('/')
            record_to_insert = (int(list_splited[6]), list_splited[7], f'{list_splited[0]}/{list_splited[1]}/{list_splited[2]}/{list_splited[3]}/{list_splited[4]}/{list_splited[5]}/{list_splited[6]}/{list_splited[7]}/master.idx', 0)
            try:
                db_cursor.execute(insert_qry, record_to_insert)
                db.commit()
            except:
                print(f'Duplicated record {record_to_insert}')
                db.rollback()
            index += 1
        db_cursor.close()
        db_cursor = db.cursor()
    qry_qtr_table_sorted = '''SELECT * FROM QTR_INDEX WHERE CRAWLED = 1 ORDER BY YEAR, QUARTER'''
    db_cursor.execute(qry_qtr_table_sorted)
    table_sorted = db_cursor.fetchall()
    last_year = table_sorted[-1][1]
    last_qtr = table_sorted[-1][2]
    tmp_year = last_year
    tmp_qtr = int(last_qtr[-1])
    while tmp_year != cur_year:
        if tmp_qtr == 4:
            tmp_qtr = 0
            tmp_year += 1
        tmp_qtr += 1
        record_to_insert = (tmp_year, f'QTR{tmp_qtr}', f'{list_splited[0]}/{list_splited[1]}/{list_splited[2]}/{list_splited[3]}/{list_splited[4]}/{list_splited[5]}/{tmp_year}/QTR{tmp_qtr}/master.idx', 0)
        try:
            db_cursor.execute(insert_qry, record_to_insert)
            db.commit()
        except:
            print(f'Duplicated record {record_to_insert}')
            db.rollback()
        index += 1
    db_cursor.close()
    db_cursor = db.cursor()
    if tmp_qtr == 4:
        tmp_qtr = 0
    while tmp_qtr != cur_qtr:
        tmp_qtr += 1
        record_to_insert = tmp_year, f'QTR{tmp_qtr}', f'{list_splited[0]}/{list_splited[1]}/{list_splited[2]}/{list_splited[3]}/{list_splited[4]}/{list_splited[5]}/{tmp_year}/QTR{tmp_qtr}/master.idx'
        try:
            db_cursor.execute(insert_qry, record_to_insert)
            db.commit()
        except:
            print(f'Duplicated record {record_to_insert}')
            db.rollback()
        index += 1
    db_cursor.close()
    master_idx_updater(db)
    db.close()



if __name__ == '__main__':
    main()
