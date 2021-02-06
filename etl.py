# Import Python packages
import traceback

import os
import glob
import csv
from cassandra.cluster import Cluster

t_music_by_user = "T_MUSIC_BY_USER"
t_user_by_song = "T_USER_BY_SONG"
t_music = "T_MUSIC"


class ETLCassandra:
    def __init__(self, file_path, connection_url):
        self.file_path = file_path
        self.connection_url = connection_url
        self.session = None
        pass

    @staticmethod
    def get_list_of_file_paths():
        """
             Gets list of file paths from event data folder
        """
        # checking your current working directory
        print(os.getcwd())
        path_list = None
        # Get your current folder and sub-folder event data
        filepath = os.getcwd() + '/event_data'

        # Create a for loop to create a list of files and collect each filepath
        for root, dirs, files in os.walk(filepath):
            # join the file path and roots with the subdirectories using glob
            path_list = glob.glob(os.path.join(root, '*'))

        return path_list

    def create_csv(self, file_list=None):
        """
             Loop through all event data files and aggregate them in a CSV
        """
        # initiating an empty list of rows that will be generated from each file
        full_data_rows_list = []

        # for every filepath in the file path list
        for f in file_list:

            # reading csv file
            with open(f, 'r', encoding='utf8', newline='') as csv_file:
                # creating a csv reader object
                csv_reader = csv.reader(csv_file)
                next(csv_reader)

                # extracting each data row one by one and append it
                for line in csv_reader:
                    # print(line)
                    full_data_rows_list.append(line)

        # create a smaller event data csv file called event_datafile_full csv \
        # that will be used to insert data into the  Apache Cassandra tables
        csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

        with open(self.file_path, 'w', encoding='utf8', newline='') as f:
            writer = csv.writer(f, dialect='myDialect')
            writer.writerow(['artist', 'firstName', 'gender', 'itemInSession', 'lastName', 'length',
                             'level', 'location', 'sessionId', 'song', 'userId'])
            for row in full_data_rows_list:
                if row[0] == '':
                    continue
                writer.writerow(
                    (row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

    def create_set_keyspace(self):
        """
             Create Connection to Cassandra DB
             Create and set key-space
        """
        cluster = Cluster([self.connection_url])

        # To establish connection and begin executing queries, need a session
        self.session = cluster.connect()

        # Create a Keyspace
        try:
            self.session.execute("""
            CREATE KEYSPACE IF NOT EXISTS udacity 
            WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
            """)
        except Exception as e:
            print(e)

        # Set KEYSPACE to the keyspace specified above
        try:
            self.session.set_keyspace('udacity')
        except Exception as e:
            print(e)
        return self.session, cluster

    def create_table(self, table_name, pk_combination, suffix=""):
        """
             Create tables with dynamic queries
        """
        query = "CREATE TABLE IF NOT EXISTS " + table_name
        query = query + "(artist text, first_name text, gender text, item_in_session int, last_name text, \
        length float, level text, location text, session_id int,song text,user_id int, \
        PRIMARY KEY(" + pk_combination + "))" + suffix
        try:
            print(query)
            self.session.execute(query)
            print(f'{table_name} Created successfully')
        except Exception as e:
            print(e)

    def populate_tables(self, table_name):
        """
             Insert data to tables with dynamic queries
        """
        with open(self.file_path, encoding='utf8') as f:
            csv_reader = csv.reader(f)
            next(csv_reader)  # skip header
            for line in csv_reader:
                # TO-DO: Assign the INSERT statements into the `query` variable
                query = "INSERT INTO " + table_name + "(artist, first_name, gender,item_in_session,last_name, length, \
                level, location, session_id,song,user_id)"
                query = query + "values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                # Assign which column element should be assigned for each column in the INSERT statement.
                self.session.execute(query,
                                     (
                                         line[0], line[1], line[2], int(line[3]), line[4], float(line[5]), line[6],
                                         line[7],
                                         int(line[8]), line[9], int(line[10])))
            print(f'Data inserted successfully to {table_name}')

    def test_query(self, query_list):
        """
             Test the given queries in Notebook
        """
        for q in query_list:
            try:
                print(f'Query : {q}')
                result = self.session.execute(q)
                for row in result:
                    print(row)
            except Exception as e:
                print(e)
                print("Error testing query " + str(traceback.format_exc()))

    def drop_tables(self, table_name):
        """
             Drop all tables
        """
        drop_query = f'DROP TABLE {table_name}'
        try:
            self.session.execute(drop_query)
            print(f'{table_name} was dropped')
        except Exception as e:
            print(e)
            print("Error dropping table " + str(traceback.format_exc()))


def main():
    """
         Wrapper function to sequentially execute all ETL methods
    """

    etl = ETLCassandra('event_datafile_new.csv', connection_url='127.0.0.1')

    # Process data
    file_path_list = etl.get_list_of_file_paths()
    etl.create_csv(file_path_list)
    session, cluster = etl.create_set_keyspace()

    # Create the tables
    etl.create_table(t_music, pk_combination="session_id, item_in_session")
    etl.create_table(t_music_by_user, pk_combination="(user_id, session_id) , item_in_session")
    etl.create_table(t_user_by_song, pk_combination="song, user_id")

    # Insert data to tables
    etl.populate_tables(t_music)
    etl.populate_tables(t_music_by_user)
    etl.populate_tables(t_user_by_song)

    # Test query
    query1 = "select artist,song, length from T_MUSIC where session_id=338 and item_in_session = 4"
    query2 = "select artist,song,first_name,last_name,item_in_session from T_MUSIC_BY_USER where user_id = 10 and \
     session_id = 182"
    query3 = "select first_name,last_name from T_USER_BY_SONG where song = 'All Hands Against His Own'"
    etl.test_query([query1, query2, query3])

    # Drop tables
    etl.drop_tables(t_music)
    etl.drop_tables(t_music_by_user)
    etl.drop_tables(t_user_by_song)

    # check the number of rows in your csv file
    with open('event_datafile_new.csv', 'r', encoding='utf8') as f:
        print(f'Total number of rows in CSV : {sum(1 for line in f)}')

    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
