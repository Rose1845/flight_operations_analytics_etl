
import psycopg2

from api_request import fetch_data


def conect_to_db():
    print("Connecting POSTGReS !!!")
    try:
        conn = psycopg2.connect(
            "host=db port=5432 password=password dbname=flightoperations user=postgres")
        print(conn)
        return conn
    except psycopg2.Error as e:
        print(f"Database conection failed! {e}")


def create_table(conn):
    print("Creating table")
    try:
        cursor = conn.cursor()

        cursor.execute("CREATE SCHEMA IF NOT EXISTS flight;")
        conn.commit()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS flight.flight_snapshots (
                id            BIGSERIAL   PRIMARY KEY,
                snapshot_time INTEGER     NOT NULL,
                fetched_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        conn.commit()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS flight.state_vectors (
                id              BIGSERIAL        PRIMARY KEY,
                snapshot_id     BIGINT           NOT NULL
                                    REFERENCES flight.flight_snapshots(id)
                                    ON DELETE CASCADE,
                icao24          VARCHAR(6)       NOT NULL,
                callsign        VARCHAR(8),
                origin_country  TEXT,
                time_position   INTEGER,
                last_contact    INTEGER,
                longitude       DOUBLE PRECISION,
                latitude        DOUBLE PRECISION,
                baro_altitude   DOUBLE PRECISION,
                on_ground       BOOLEAN,
                velocity        DOUBLE PRECISION,
                true_track      DOUBLE PRECISION,
                vertical_rate   DOUBLE PRECISION,
                sensors         INTEGER[],
                geo_altitude    DOUBLE PRECISION,
                squawk          VARCHAR(4),
                spi             BOOLEAN,
                position_source SMALLINT,
                category        SMALLINT
            );
        """)
        conn.commit()

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_sv_icao24   ON flight.state_vectors(icao24);")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_sv_snapshot ON flight.state_vectors(snapshot_id);")
        conn.commit()

        print("Tables created successfully.")

    except psycopg2.Error as e:
        print(f"Failed to create tables! {e}")
        conn.rollback()
        raise


conn = conect_to_db()
create_table(conn)


def insert_data(conn, data):
    print("Inserting flight operation data")
    snap_time = data.get("time")
    print(snap_time)
    states = data.get("states") or []
    print(states)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO flight.flight_snapshots (snapshot_time) VALUES (%s) RETURNING id",
            (snap_time,)
        )
        snapshot_id = cursor.fetchone()[0]
        print(snapshot_id)
        conn.commit()
        print("Fight Shot Data inserted correctly")

        for state in states:
            cursor.execute("""
                INSERT INTO flight.state_vectors (
                    snapshot_id, icao24, callsign, origin_country,
                    time_position, last_contact,
                    longitude, latitude, baro_altitude,
                    on_ground, velocity, true_track, vertical_rate,
                    sensors, geo_altitude, squawk,
                    spi, position_source, category
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s
                )
            """, (
                snapshot_id,
                state[0],
                (state[1] or "").strip() or None,
                state[2],
                state[3],
                state[4],
                state[5],
                state[6],
                state[7],
                state[8],
                state[9],
                state[10],
                state[11],
                state[12],
                state[13],
                state[14],
                state[15],
                state[16],
                state[17] if len(state) > 17 else None
            ))

        conn.commit()
        print("Flight State Vectors inserted correctly")

    except psycopg2.Error as e:
        print(f"Failed to insert tables! {e}")
        conn.rollback()
        raise


def main():
    try:
        data = fetch_data()
        conn = conect_to_db()
        insert_data(conn, data)
    except Exception as e:
        print(f"An error occured during an execution{e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed!!")
