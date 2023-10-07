import mysql.connector as connector #import connector class
from mysql.connector import pooling #import pooling class
from mysql.connector import Error  # Import  error class
try:
    connection=connector.connect(user="root",password="TermsOfUse5477#") #create connection with username and password 
    cursor = connection.cursor() #create the cursor 
    try: #attempts to create the database, and give an error if it fails for any reason (such as it already being created)
        cursor.execute("CREATE DATABASE little_lemon_db") 
    
    except connector.Error as err:
        print("Error:", err) 
    cursor.execute("USE little_lemon_db") #starts using the database, so that it can be populated 
    
    #each of these sets the specifications of the table 
    create_menuitem_table = """CREATE TABLE MenuItems (
    ItemID INT AUTO_INCREMENT,
    Name VARCHAR(200),
    Type VARCHAR(100),
    Price INT,
    PRIMARY KEY (ItemID)
    );"""

    create_menu_table = """CREATE TABLE Menus (
    MenuID INT,
    ItemID INT,
    Cuisine VARCHAR(100),
    PRIMARY KEY (MenuID,ItemID)
    );"""

    create_booking_table = """CREATE TABLE Bookings (
    BookingID INT AUTO_INCREMENT,
    TableNo INT,
    GuestFirstName VARCHAR(100) NOT NULL,
    GuestLastName VARCHAR(100) NOT NULL,
    BookingSlot TIME NOT NULL,
    EmployeeID INT,
    PRIMARY KEY (BookingID)
    );"""

    create_orders_table = """CREATE TABLE Orders (
    OrderID INT,
    TableNo INT,
    MenuID INT,
    BookingID INT,
    BillAmount INT,
    Quantity INT,
    PRIMARY KEY (OrderID,TableNo)
    );"""

    create_employees_table = """CREATE TABLE Employees (
    EmployeeID INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR,
    Role VARCHAR,
    Address VARCHAR,
    Contact_Number INT,
    Email VARCHAR,
    Annual_Salary VARCHAR
    );"""
    try: # Creates each table, and gives an error if any fail         
        # I could make multiple try blocks if I want  it to try and catch every table instead of the first time an error occurs. 
        # Create MenuItems table
        cursor.execute(create_menuitem_table)

        # Create Menu table
        cursor.execute(create_menu_table)

        # Create Bookings table
        cursor.execute(create_booking_table)

        # Create Orders table
        cursor.execute(create_orders_table)

        # Create Employees table
        cursor.execute(create_employees_table)
    except connector.Error as err:
        print("Error:", err)
        # Below is the data to insert into each table
    insert_menuitems="""
    INSERT INTO MenuItems (ItemID, Name, Type, Price)
    VALUES
    (1, 'Olives','Starters',5),
    (2, 'Flatbread','Starters', 5),
    (3, 'Minestrone', 'Starters', 8),
    (4, 'Tomato bread','Starters', 8),
    (5, 'Falafel', 'Starters', 7),
    (6, 'Hummus', 'Starters', 5),
    (7, 'Greek salad', 'Main Courses', 15),
    (8, 'Bean soup', 'Main Courses', 12),
    (9, 'Pizza', 'Main Courses', 15),
    (10, 'Greek yoghurt','Desserts', 7),
    (11, 'Ice cream', 'Desserts', 6),
    (12, 'Cheesecake', 'Desserts', 4),
    (13, 'Athens White wine', 'Drinks', 25),
    (14, 'Corfu Red Wine', 'Drinks', 30),
    (15, 'Turkish Coffee', 'Drinks', 10),
    (16, 'Turkish Coffee', 'Drinks', 10),
    (17, 'Kabasa', 'Main Courses', 17);"""

    insert_menu="""
    INSERT INTO Menus (MenuID,ItemID,Cuisine)
    VALUES
    (1, 1, 'Greek'),
    (1, 7, 'Greek'),
    (1, 10, 'Greek'),
    (1, 13, 'Greek'),
    (2, 3, 'Italian'),
    (2, 9, 'Italian'),
    (2, 12, 'Italian'),
    (2, 15, 'Italian'),
    (3, 5, 'Turkish'),
    (3, 17, 'Turkish'),
    (3, 11, 'Turkish'),
    (3, 16, 'Turkish');"""

    insert_bookings="""
    INSERT INTO Bookings (BookingID, TableNo, GuestFirstName, 
    GuestLastName, BookingSlot, EmployeeID)
    VALUES
    (1, 12, 'Anna','Iversen','19:00:00',1),
    (2, 12, 'Joakim', 'Iversen', '19:00:00', 1),
    (3, 19, 'Vanessa', 'McCarthy', '15:00:00', 3),
    (4, 15, 'Marcos', 'Romero', '17:30:00', 4),
    (5, 5, 'Hiroki', 'Yamane', '18:30:00', 2),
    (6, 8, 'Diana', 'Pinto', '20:00:00', 5);"""


    insert_orders="""
    INSERT INTO Orders (OrderID, TableNo, MenuID, BookingID, Quantity, BillAmount)
    VALUES
    (1, 12, 1, 1, 2, 86),
    (2, 19, 2, 2, 1, 37),
    (3, 15, 2, 3, 1, 37),
    (4, 5, 3, 4, 1, 40),
    (5, 8, 1, 5, 1, 43);"""


    insert_employees ="""
    INSERT INTO employees (EmployeeID, Name, Role, Address, Contact_Number, Email, Annual_Salary)
    (01,'Mario Gollini','Manager','724, Parsley Lane, Old Town, Chicago, IL',351258074,'Mario.g@littlelemon.com','$70,000'),
    (02,'Adrian Gollini','Assistant Manager','334, Dill Square, Lincoln Park, Chicago, IL',351474048,'Adrian.g@littlelemon.com','$65,000'),
    (03,'Giorgos Dioudis','Head Chef','879 Sage Street, West Loop, Chicago, IL',351970582,'Giorgos.d@littlelemon.com','$50,000'),
    (04,'Fatma Kaya','Assistant Chef','132  Bay Lane, Chicago, IL',351963569,'Fatma.k@littlelemon.com','$45,000'),
    (05,'Elena Salvai','Head Waiter','989 Thyme Square, EdgeWater, Chicago, IL',351074198,'Elena.s@littlelemon.com','$40,000'),
    (06,'John Millar','Receptionist','245 Dill Square, Lincoln Park, Chicago, IL',351584508,'John.m@littlelemon.com','$35,000');"""
    try: # These populate each table with the data above 
        # Populate MenuItems table
        cursor.execute(insert_menuitems)
        connection.commit()

        # Populate MenuItems table
        cursor.execute(insert_menu)
        connection.commit()

        # Populate Bookings table
        cursor.execute(insert_bookings)
        connection.commit()

        # Populate Orders table
        cursor.execute(insert_orders)
        connection.commit()

        # Populate Employees table
        cursor.execute(insert_employees)
        connection.commit()
    except connector.Error as err:
        print("Error:", err)
        
    try:
        # Sets up the connection configuration
        dbconfig={"database":"little_lemon_db", "user":"root", "password":"TermsOfUse5477#"}

        # Creates one connection pool with two connections
        connection_pool = pooling.MySQLConnectionPool(pool_name="pool_b", pool_size=2, **dbconfig)

        # Get two connections from the pool
        connection1 = connection_pool.get_connection()
        connection2 = connection_pool.get_connection()
    
        cursor = connection1.cursor()
        #defines a stored procedure to show the peak hours.
        create_procedure_query = """ 
        CREATE PROCEDURE PeakHours()
        BEGIN
            SELECT HOUR(BookingSlot) AS HourOfDay, COUNT(*) AS BookingsCount
            FROM Bookings
            GROUP BY HourOfDay
            ORDER BY BookingsCount DESC;
        END;
        """
        try: #attempts to create the stored procedure and give an error if needed
            cursor.execute(create_procedure_query)
        except connector.Error as err:
            print("Error:", err)
        #defines a stored procedure to show the guest status 
        create_procedure_query = """
   
        CREATE PROCEDURE GuestStatus() 
        BEGIN
            SELECT
                CONCAT(Booking.FirstName, ' ', Booking.LastName) AS GuestName,
                CASE
                    WHEN Employees.Role IN ('Manager', 'Assistant Manager') THEN 'Ready to pay'
                    WHEN Employees.Role = 'Head Chef' THEN 'Ready to serve'
                    WHEN Employees.Role = 'Assistant Chef' THEN 'Preparing Order'
                    WHEN Employees.Role = 'Head Waiter' THEN 'Order served'
                END AS GuestOrderStatus
            FROM Bookings
            LEFT JOIN Employees ON Bookings.EmployeeID = Employees.EmployeeID;
        END;
        """
        try: #creates the above stored procedure
            cursor.execute(create_procedure_query)
        except connector.Error as err:
            print("Error:", err)

        # When done, release the connections back to the pool
        connection1.close()
        connection2.close()

    except Error as err:
        if err.errno == 1304:  # Check if the error is due to an existing procedure (error code 1304)
            print("Procedure already exists. Continuing...")
        else:
            print("Error:", err)
            

except connector.Error as err: #catches any unforseet errors 
    print("Error:", err)
finally: #closes the cursor and connection 
    cursor.close()
    connection.close()
try: #performs each of the listed operations for the little lemon meta project 
    # Get a connection from the pool
    connection = connection_pool.get_connection()
    cursor = connection.cursor()

    # 1. The name and EmployeeID of the Little Lemon manager.
    cursor.execute("SELECT EmployeeName, EmployeeID FROM Employees WHERE Role = 'Manager'")
    manager_data = cursor.fetchone()
    print("Little Lemon Manager:", manager_data)

    # 2. The name and role of the employee who receives the highest salary.
    cursor.execute("SELECT EmployeeName, Role FROM Employees ORDER BY Salary DESC LIMIT 1")
    highest_salary_employee = cursor.fetchone()
    print("Employee with Highest Salary:", highest_salary_employee)

    # 3. The number of guests booked between 18:00 and 20:00.
    cursor.execute("SELECT COUNT(*) FROM Bookings WHERE HOUR(BookingSlot) BETWEEN 18 AND 20")
    guests_booked_count = cursor.fetchone()[0]
    print("Number of Guests Booked between 18:00 and 20:00:", guests_booked_count)

    # 4. The full name and BookingID of all guests waiting to be seated with the receptionist in sorted order.
    cursor.execute("SELECT CONCAT(FirstName, ' ', LastName) AS GuestName, BookingID FROM Bookings WHERE Status = 'Waiting' ORDER BY BookingSlot")
    waiting_guests = cursor.fetchall()
    print("Guests Waiting to Be Seated:")
    for guest in waiting_guests:
        print(guest)
    # Task 5: Display the next three upcoming bookings
    try:
        # Get a connection from the pool
        connection = connection_pool.get_connection()
        cursor = connection.cursor(buffered=True)

        # Combine data from Bookings and Employee tables and sort by BookingSlot
        cursor.execute("""
        SELECT BookingSlot, CONCAT(FirstName, ' ', LastName) AS GuestName, CONCAT('Assigned to: ', EmployeeName, ' [', Role, ']') AS AssignedTo
        FROM Bookings
        LEFT JOIN Employees ON Bookings.EmployeeID = Employees.EmployeeID
        WHERE BookingSlot >= NOW()
        ORDER BY BookingSlot
        LIMIT 3
        """)

        upcoming_bookings = cursor.fetchall()

        # Display the information
        for booking in upcoming_bookings:
            print("[BookingSlot]:", booking[0])
            print("[Guest_name]:", booking[1])
            print("[Assigned to:", booking[2], "]")
            print()

    except Error as err:
        print("Error:", err)

    finally:
        # Return the connection back to the pool
        cursor.close()
        connection.close()
except Error as err:
    print("Error:", err)
finally:
    # Return the connection back to the pool
    cursor.close()
    connection.close()

