import os
import sys

abs_pwd = os.path.dirname(os.path.abspath(__file__))
sys.path.append(abs_pwd)
sys.path.append(os.path.join(abs_pwd, ".."))

from mysql_test import mysql_conn


def test_func():
    host_ip = "172.24.205.230"
    # host_ip = "127.0.0.1"
    sql_agent = mysql_conn.SqlAgent(host=host_ip, port=3306, user='dhu_test', passwd='123456', db_name='mysql')
    sql_agent.connect_db()
    sql_agent.get_all_user_and_host()
    sql_agent.get_all_user_and_host_fetchone()
    sql_agent.get_all_user_and_host_dict()

    # create employee database
    sql_agent.create_db(db_name="employee_db")
    sql_agent.connect_db()
    sql_agent.create_table("em_meta_table")
    sql_agent.insert_table(last_name="Green", first_name="Jim", age=21)
    sql_agent.insert_table(last_name="Yellow", first_name="Jim", age=22)
    sql_agent.insert_table(last_name="Red", first_name="Jim", age=23)
    sql_agent.select_all()
    sql_agent.update_table()
    sql_agent.select_by_id(19)
    sql_agent.delete_by_lastname("Green")
    sql_agent.select_all()


if __name__ == "__main__":
    test_func()
