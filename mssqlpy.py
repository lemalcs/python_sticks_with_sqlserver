import mssql_python
import pytest

def test_connect_to_bare_sql_server():
    connection_string = "SERVER=localhost\\sql2025ed;DATABASE=master;Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes"
    connection = mssql_python.connect(connection_string)

    cursor = connection.cursor()
    cursor.execute("select count(1) from sys.all_objects")
    rows = cursor.fetchall()
    rows_number = rows[0][0]

    print(f"Number of rows in sys.all_objects: {rows_number}")

    assert rows_number > 0

@pytest.fixture()
def db_connection():
    connection_string = "SERVER=localhost\\sql2025ed;DATABASE=Household;Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes"
    connection = mssql_python.connect(connection_string)
    return connection

def test_create_table(db_connection):
    statement="""
drop table if exists dbo.Chores
create table dbo.Chores
(
    Id int,
    Title nvarchar(256),
    DueBy date,
    IsComplete bit
)
"""
    cursor=db_connection.cursor()
    cursor.execute(statement)

    cursor.execute("select count(1) from sys.tables where name='Chores'")
    rows=cursor.fetchone()

    assert rows[0]==1

def test_insert_and_query_data(db_connection):
    insert_statement="""
insert into dbo.Chores (Id, Title, DueBy, IsComplete) 
values(?, ?, ?, ?)
"""
    cursor=db_connection.cursor()
    cursor.execute(insert_statement,
                   (
                       1,
                       "Take out the trash",
                       "2025-09-16",
                       0
                   ))
    
    cursor.execute("select Title, DueBy, IsComplete from dbo.Chores where Id=?", (1,))
    
    row=cursor.fetchone()
    cursor.commit()

    assert row[0]=="Take out the trash"
    assert str(row[1])=="2025-09-16"
    assert row[2]==0

def test_execute_stored_procedure(db_connection):

    """ 
    /* Source code for stored procedure */
    create proc dbo.save_chore
            @Title nvarchar(256),
            @DueBy date,
            @IsComplete bit,
            @NewId int output
    as
        begin
            select @NewId=count(1)+1 from dbo.Chores

            insert dbo.Chores(Id, Title, DueBy, IsComplete)
            values(@NewId,@Title,@DueBy,@IsComplete)
        end
    """

    statement="""
set nocount on
declare @new_id int
exec dbo.save_chore @Title=?, @DueBy=?, @IsComplete=?, @NewId=@new_id output
select @new_id
"""

    params=(
        "Mow the lawn",
        "2025-09-20",
        0
    )
    cursor=db_connection.cursor()
    cursor.execute(statement, params)
    rows=cursor.fetchall()
    for row in rows:
        new_id=row[0]
        break
    cursor.commit()

    assert new_id is not None

def test_execute_function(db_connection):

    """ 
    /* Source code for function */
create function dbo.translate_to_emoji(@word nvarchar(32))
returns nvarchar(32)
as
    begin
        if @word = 'dog'
            return N'🐕'

        if @word = 'dishes'
            return N'🍽️'

        if @word = 'laundry'
            return N'👕'

        if @word = 'bathroom'
            return N'🚽'

        if @word = 'car'
            return N'🛻'

        return @word
    end
    """

    statement="select dbo.translate_to_emoji(?)"
    cursor=db_connection.cursor()
    cursor.execute(statement, ("dog",))

    row=cursor.fetchone()
    emoji=None
    while row:
        emoji=row[0]
        break

    assert emoji=="🐕"


def get_chores():
    chores_for_everyday=[
        (2, "Wash the dishes", "2025-09-17", 0),
        (3, "Do the laundry", "2025-09-18", 0),
        (4, "Clean the bathroom", "2025-09-19", 0),
        (5, "Wash the car", "2025-09-21", 0)
    ]

    all_chores=[]

    for id in range(0,5000):
        all_chores.append((
            id+1,
            chores_for_everyday[id % len(chores_for_everyday)][1],
            "2025-12-31",
            0
        ))

    return all_chores

def test_fake_bulk_insert(db_connection):
    statement="""
insert into dbo.Chores (Id, Title, DueBy, IsComplete) 
values(?, ?, ?, ?)"""

    cursor=db_connection.cursor()
    chores= get_chores()
    cursor.executemany(statement,chores)
    cursor.commit()
    print(f"Inserted {len(chores)} rows.")

    cursor.execute("select count(1) from dbo.Chores")
    row=cursor.fetchone()

    assert row[0]>=100000