from airflow.models import Connection
from airflow.utils.session import provide_session
from sqlalchemy.exc import IntegrityError

@provide_session
def create_connections(session=None):
    connections = [
        Connection(
            conn_id='webhdfs_default',
            conn_type='webhdfs',
            host='namenode',
            port=9870,
            extra='{"use_ssl": false}'
        ),
        Connection(
            conn_id='hiveserver2_default',
            conn_type='hiveserver2',
            host='hive-server',
            port=10000,
            login='hive',
            password='hive',
            schema='default'
        )
    ]

    for conn in connections:
        try:
            session.add(conn)
            session.commit()
        except IntegrityError:
            session.rollback()
            existing_conn = session.query(Connection).filter_by(conn_id=conn.conn_id).first()
            if existing_conn:
                existing_conn.set_extra(conn.extra)
                existing_conn.set_password(conn.password)
                session.merge(existing_conn)
                session.commit()
        except Exception as e:
            session.rollback()
            raise e

    print("Connections created or updated successfully")