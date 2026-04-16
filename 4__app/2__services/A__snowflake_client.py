import snowflake.connector
from getpass import getpass


##def get_connection():
##    account = input("Snowflake account: ")
##    user = input("Snowflake user: ")
##    password = getpass("Snowflake password: ")
##
##    conn = snowflake.connector.connect(
##        account=account,
##        user=user,
##        password=password,
##        role="ACCOUNTADMIN",
##        warehouse="COMPUTE_WH",
##        database="SNOWFLAKE",
##        schema="ACCOUNT_USAGE"
##    )
##
##    return conn

def get_connection():
    account = "PFCBSOA-DQ82916"   # hardcode
    user = "MCAVAGNA"         # hardcode
    password = "Larraya2512!.."  #getpass("Snowflake password: ")

    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        role="ACCOUNTADMIN",
        warehouse="COMPUTE_WH",
        database="SNOWFLAKE",
        schema="ACCOUNT_USAGE"
    )

    return conn


def test_connection():
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_VERSION()")
        result = cur.fetchone()

        print("\n✅ CONNECTION SUCCESSFUL")
        print("User / Role / Version:")
        print(result)

    except Exception as e:
        print("\n❌ CONNECTION FAILED")
        print(str(e))

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    test_connection()