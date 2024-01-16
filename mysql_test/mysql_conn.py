import logging
import pymysql
import glog


class SqlAgent:
    def __init__(self, host: str = "127.0.0.1", port: int = 3306, user: str = "root", passwd: str = "",
                 db_name: str = "",
                 charset="utf8mb4"):
        self.host_ = host
        self.port_ = port
        self.user_ = user
        self.passwd_ = passwd
        self.db_name_ = db_name
        self.charset_ = charset
        self.db_conn: pymysql.connections.Connection = None
        self.table_name_: str = ""

    def __del__(self):
        if self.db_conn:
            self.db_conn.close()

    def connect_db(self):
        glog.info("==========================")
        try:
            # close database
            self.close_db()

            # connect database
            self.db_conn = pymysql.connect(host=self.host_, port=self.port_, user=self.user_, password=self.passwd_,
                                           database=self.db_name_, charset=self.charset_)

            if self.db_conn is not None:
                logging.info(
                    "connect db:{}, host:{}, port{}, user:{}, passwd:{}".format(self.db_name_, self.host_,
                                                                                self.port_,
                                                                                self.user_,
                                                                                self.passwd_))
            else:
                logging.info(
                    "error when connecting db:{}, host:{}, port{}, user:{}, passwd:{}".format(self.db_name_,
                                                                                              self.host_,
                                                                                              self.port_,
                                                                                              self.user_,
                                                                                              self.passwd_))
        except Exception as e:
            self.db_conn = None
            logging.info("except {}".format(str(e)))

    def close_db(self):
        if self.db_conn:
            self.db_conn.close()

    def connect_sql(self):
        glog.info("==========================")
        try:
            self.close_db()

            # connect database
            self.db_conn = pymysql.connect(host=self.host_, port=self.port_, user=self.user_, password=self.passwd_,
                                           charset=self.charset_)

            if self.db_conn is not None:
                logging.info(
                    "connect host:{}, port{}, user:{}, passwd:{}".format(self.host_, self.port_,
                                                                         self.user_,
                                                                         self.passwd_))
            else:
                logging.info(
                    "error when connecting host:{}, port{}, user:{}, passwd:{}".format(self.host_,
                                                                                       self.port_,
                                                                                       self.user_,
                                                                                       self.passwd_))
        except Exception as e:
            self.db_conn = None
            logging.info("except {}".format(str(e)))

    def get_all_user_and_host(self):
        glog.info("==========================")
        with self.db_conn.cursor() as db_cursor:
            ret = db_cursor.execute('select User, Host from user;')
            glog.info("execute ret:{}, typeof ret:{}".format(ret, type(ret)))
            results = db_cursor.fetchall()
            glog.info("db_cursor.fetchall() results:{}, typeof results:{}".format(results, type(results)))
            result = db_cursor.fetchone()
            while result is not None:
                glog.info("db_cursor.fetchone() result:{}, typeof result:{}".format(result, type(result)))

    def get_all_user_and_host_dict(self):
        glog.info("==========================")
        with self.db_conn.cursor(pymysql.cursors.DictCursor) as db_cursor:
            ret = db_cursor.execute('select User, Host from user;')
            glog.info("execute ret:{}, typeof ret:{}".format(ret, type(ret)))
            results = db_cursor.fetchall()
            glog.info("db_cursor.fetchall() results:{}, typeof results:{}".format(results, type(results)))
            result = db_cursor.fetchone()
            while result is not None:
                glog.info("db_cursor.fetchone() result:{}, typeof result:{}".format(result, type(result)))

    def get_all_user_and_host_fetchone(self):
        glog.info("==========================")
        with self.db_conn.cursor() as db_cursor:
            ret = db_cursor.execute("select User, Host, password_last_changed from user where User='dhu_test'")
            glog.info("execute ret:{}, typeof ret:{}".format(ret, type(ret)))
            result = db_cursor.fetchone()
            while result is not None:
                glog.info("db_cursor.fetchone() result:,{} typeof result:{}".format(result, type(result)))
                result = db_cursor.fetchone()

    def if_db_exists(self, db_name: str) -> bool:
        if self.db_conn:
            with self.db_conn.cursor() as cursor:
                db_already_exists = False

                # check if database exist
                show_database_cmd = "show databases"
                ret = cursor.execute(show_database_cmd)
                db_all = cursor.fetchall()
                glog.info("all databases:{}".format(db_all))
                for each_result in db_all:
                    if self.db_name_ in each_result:
                        db_already_exists = True
                        return True
        else:
            glog.info("connection with mysql lost.")
            return False
        return False

    def __create_db_imp__(self):
        with self.db_conn.cursor() as cursor:
            create_db_cmd = "create database if not exists {}".format(self.db_name_)
            ret = cursor.execute(create_db_cmd)
            glog.info("create db ret:{}, cmd:{}".format(ret, create_db_cmd))

    def create_db(self, db_name: str = "employee_db") -> bool:
        glog.info("==========================")
        self.db_name_ = db_name
        self.connect_sql()

        self.__create_db_imp__()

        return True

    def create_table(self, table_name: str):
        glog.info("==========================")
        if self.db_conn:
            with self.db_conn.cursor() as cursor:
                create_table_cmd = ("create table if not exists {}("
                                    "em_id int auto_increment,"
                                    "em_last_name varchar(255),"
                                    "em_first_name varchar(255),"
                                    "em_age int unsigned,"
                                    "primary key (em_id)"
                                    ")".format(table_name))
                cursor.execute(create_table_cmd)
                self.table_name_ = table_name
                glog.info("create_table_cmd:{}".format(create_table_cmd))
        else:
            glog.info("connection with mysql lost.")
            return False

    def insert_table(self, last_name: str, first_name: str, age: int):
        glog.info("==========================")
        if self.db_conn:
            with self.db_conn.cursor() as cursor:
                insert_cmd = "insert into {}(em_last_name, em_first_name, em_age) values('{}','{}',{})".format(
                    self.table_name_, last_name, first_name, age)
                try:
                    cursor.execute(insert_cmd)
                    self.db_conn.commit()
                except:
                    self.db_conn.rollback()
                glog.info("insert_cmd:{}".format(insert_cmd))

    def select_all(self):
        glog.info("==========================")
        if self.db_conn:
            with self.db_conn.cursor(pymysql.cursors.DictCursor) as cursor:
                select_cmd = "select * from {}.{}".format(self.db_name_, self.table_name_)
                ret = cursor.execute(select_cmd)
                results = cursor.fetchall()
                glog.info("select_cmd:{}.\nret:{}, results:\n\t{}".format(select_cmd, ret, results))

    def select_by_id(self, em_id: int):
        glog.info("==========================")
        if self.db_conn:
            with self.db_conn.cursor() as cursor:
                select_cmd = "select * from {}.{} where em_id={}".format(self.db_name_, self.table_name_, em_id)
                ret = cursor.execute(select_cmd)
                results = cursor.fetchall()
                glog.info("select_cmd:{}.\nret:{}, results:\n\t{}".format(select_cmd, ret, results))

    def update_table(self):
        glog.info("==========================")
        if self.db_conn:
            with self.db_conn.cursor() as cursor:
                update_cmd = "update {} set em_age = 23 where em_id = 19".format(self.table_name_)
                try:
                    cursor.execute(update_cmd)
                    self.db_conn.commit()
                    glog.info("update_cmd:{}".format(update_cmd))
                except:
                    self.db_conn.rollback()

    def delete_by_lastname(self, last_name: str):
        glog.info("==========================")
        if self.db_conn:
            with self.db_conn.cursor() as cursor:
                delete_cmd = "delete from {} where em_last_name = '{}'".format(self.table_name_, last_name)
                cursor.execute(delete_cmd)
                self.db_conn.commit()
                glog.info("delete_cmd:{}".format(delete_cmd))
