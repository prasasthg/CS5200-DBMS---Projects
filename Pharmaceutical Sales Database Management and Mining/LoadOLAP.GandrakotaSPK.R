#/ FILE DETAILS /////////////////////////////////////////////////////////////#
# CS5200 - DBMS
# TITLE: Practicum II / Load OLAP Star Schema in MySQL
# AUTHOR: SAI PRASASTH KOUNDINYA GANDRAKOTA
# NUID: 002772719
# DATE: Spring 2024
#////////////////////////////////////////////////////////////////////////////#

# Clear all objects and variables
rm(list = ls())

# Store names of packages required for this script
packages <- c("RMySQL", "RSQLite", "DBI")

# Install packages not yet installed
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages])
}

# Load all packages by applying 'library' function
invisible(lapply(packages, library, character.only = TRUE))

#///////////////////////////////////////////////////////////////////////////////////////////////////#
# Function to delete any existing tables in database
# INPUT: DB Connection
# OUTPUT: None
#///////////////////////////////////////////////////////////////////////////////////////////////////#

delete_tables <- function(con)
{
  # Delete tables in database
  dbExecute(con, "DROP TABLE IF EXISTS product_facts")
  dbExecute(con, "DROP TABLE IF EXISTS rep_facts")
}

#///////////////////////////////////////////////////////////////////////////////////////////////////#
# Function to create the product_facts and rep_facts tables in database
# INPUT: DB Connection
# OUTPUT: None
#///////////////////////////////////////////////////////////////////////////////////////////////////#

create_tables <- function(con)
{
  # Create product_facts table
  dbExecute(con, "CREATE TABLE product_facts (
                    pfID INTEGER AUTO_INCREMENT,
                    prodName TEXT,
                    territory TEXT,
                    year INTEGER,
                    quarter TEXT,
                    numSales INTEGER,
                    totalQty INTEGER,
                    totalSales INTEGER,
                    PRIMARY KEY (pfID)
                  );")
  
  # Create rep_facts table
  dbExecute(con, "CREATE TABLE rep_facts (
                    rfID INTEGER AUTO_INCREMENT,
                    repname TEXT,
                    commission FLOAT,
                    year INTEGER,
                    quarter TEXT,
                    numSales INTEGER,
                    avgSales FLOAT,
                    totalSales INTEGER,
                    PRIMARY KEY (rfID)
                  );")
}

#///////////////////////////////////////////////////////////////////////////////////////////////////#

main <- function()
{
  # Connect to SQLite DB
  lite_con <- dbConnect(SQLite(), "pharma.db")
  
  # Check the connection
  print("Displaying SQLite DB connection info:")
  print(dbGetInfo(lite_con))
  cat('\n\n')
  
  # Query to retrieve required columns from products, reps and sales tables in SQLite DB
  query_pft <- "SELECT p.prodName,
                      r.territory,
                      SUBSTR(s.date, 1, 4) AS year,
                      s.quarter,
                      COUNT(*) as numSales,
                      SUM(s.qty) AS totalQty,
                      SUM(s.total) AS totalSales
                FROM sales AS s
                JOIN products AS p
                  ON s.prodID = p.prodID
                JOIN reps AS r
                  ON s.repID = r.repID
                GROUP BY p.prodName, r.territory, year, s.quarter
                ORDER BY p.prodName, r.territory, year, s.quarter"
  
  # Execute query and store result in data frame
  pft_df <- dbGetQuery(lite_con, query_pft)
  # print(pft_df)
  
  # Query to retrieve required columns from reps and sales tables in SQLite DB
  query_rft <- "SELECT CONCAT(r.firstname, ' ', r.surname) AS repName,
                      r.commission,
                      SUBSTR(s.date, 1, 4) AS year,
                      s.quarter,
                      COUNT(*) as numSales,
                      AVG(s.total) AS avgSales,
                      SUM(s.total) AS totalSales
                FROM sales AS s
                JOIN reps AS r
                  ON s.repID = r.repID
                GROUP BY r.firstname, r.surname, year, s.quarter
                ORDER BY r.firstname, r.surname, year, s.quarter"
  
  # Execute query and store result in data frame
  rft_df <- dbGetQuery(lite_con, query_rft)
  # print(rft_df)
  
  # Disconnect from SQLite DB
  dbDisconnect(lite_con)
  
  # Connect to MySQL DB
  my_con <- dbConnect(MySQL(), 
                   host = "database-1.czc6oymseewd.us-east-1.rds.amazonaws.com",
                   user = "admin",
                   password = "8576939257",
                   dbname = "pharma_olap_db",
                   port = 3306)
  
  # Check the connection
  print("Displaying MySQL DB connection info:")
  print(dbGetInfo(my_con))
  cat('\n\n')
  
  # Delete any existing tables in MySQL DB
  delete_tables(my_con)

  # Create fact tables in MySQL DB
  create_tables(my_con)
  
  tryCatch({
    # Write data from pft_df data frame to product_facts table in DB
    dbWriteTable(my_con, "product_facts", pft_df, row.names = FALSE, append = TRUE)
    
    # Write data from rft_df data frame to rep_facts table in DB
    dbWriteTable(my_con, "rep_facts", rft_df, row.names = FALSE, append = TRUE)
    
  }, error = function(e) {
    # Print an error message if an error occurs during writing
    print("Error writing data to database tables:")
    print(e)
  })
  
  # Code to display tables after loading
  print("Displaying product_facts table:")
  pf <- dbGetQuery(my_con, "SELECT * FROM product_facts LIMIT 48;")
  print(pf)

  cat('\n\n')
  
  print("Displaying rep_facts table:")
  rf <- dbGetQuery(my_con, "SELECT * FROM rep_facts;")
  print(rf)
  
  # Disconnect from MySQL DB
  dbDisconnect(my_con)
}

#///////////////////////////////////////////////////////////////////////////////////////////////////#

# Call main function
main()