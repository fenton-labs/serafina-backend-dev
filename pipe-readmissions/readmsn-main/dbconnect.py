import sqlalchemy
import psycopg2

def create_rds_connection(connType):
    """
    connType: connection type of connection returned
        - 'sqlalchemy'
        - 'psycopg2'
    returns: connection
    Requires packages:
        psycopg2
        sqlalchemy
    
    """

    if (connType == 'psycopg2'):   
        connection = psycopg2.connect(
            database="postgres",
            user="dsandrawis",
            password="Summ3rD4ys",
            host="serafina-claims-db.cnqcm0ow2ll9.us-east-1.rds.amazonaws.com",
            port='5432'
        )
        print(f'\n{connType} connection made!\n')
        return connection
    elif (connType == 'sqlalchemy'):
        connection = sqlalchemy.create_engine(
            'postgresql+psycopg2://dsandrawis:Summ3rD4ys@serafina-claims-db.cnqcm0ow2ll9.us-east-1.rds.amazonaws.com/postgres'
        )
        print(f'\n{connType} connection made!\n')
        return connection
    else:
        print(f"{connType} not valid connection type, please enter either:")
        print("'psycopg2' or 'sqlalchemy'")
 