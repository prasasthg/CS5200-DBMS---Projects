#/ FILE DETAILS /////////////////////////////////////////////////////////////#
# CS5200 - DBMS
# TITLE: Practicum II / Load XML to SQLite DB
# AUTHOR: SAI PRASASTH KOUNDINYA GANDRAKOTA
# NUID: 002772719
# DATE: Spring 2024
#////////////////////////////////////////////////////////////////////////////#

# Clear all objects and variables
rm(list = ls())

# Store names of packages required for this script
packages <- c("XML", "RSQLite")

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
  dbExecute(con, "DROP TABLE IF EXISTS sales")
  dbExecute(con, "DROP TABLE IF EXISTS reps")
  dbExecute(con, "DROP TABLE IF EXISTS customers")
  dbExecute(con, "DROP TABLE IF EXISTS products")
}

#///////////////////////////////////////////////////////////////////////////////////////////////////#
# Function to create the products, reps, customers and sales tables in database
# INPUT: DB Connection
# OUTPUT: None
#///////////////////////////////////////////////////////////////////////////////////////////////////#

create_tables <- function(con)
{
  # Enable Foreign Key Constraints
  dbExecute(con, "PRAGMA foreign_keys = ON;")
  
  # Create Products table
  # prodID - Synthetic PK; 
  # prodName - Name of Product
  dbExecute(con, "CREATE TABLE products (
                    prodID INTEGER,
                    prodName TEXT,
                    PRIMARY KEY (prodID)
                  );")
  
  # Create Reps table
  dbExecute(con, "CREATE TABLE reps (
                    repID INTEGER,
                    firstname TEXT,
                    surname TEXT,
                    territory TEXT,
                    commission NUMERIC CHECK (commission > 0),
                    PRIMARY KEY (repID)
                  );")
  
  # Create Customers table
  dbExecute(con, "CREATE TABLE customers (
                    custID INTEGER,
                    custName TEXT,
                    country TEXT,
                    PRIMARY KEY (custID)
                  );")
  
  # Create Sales table
  dbExecute(con, "CREATE TABLE sales (
                    saleID INTEGER,
                    txnID INTEGER,
                    repID INTEGER,
                    custID INTEGER,
                    prodID INTEGER,
                    date DATE,
                    quarter TEXT,
                    qty INTEGER CHECK (qty > 0),
                    total INTEGER CHECK (total > 0),
                    currency TEXT DEFAULT 'USD',
                    PRIMARY KEY (saleID),
                    FOREIGN KEY (repID) REFERENCES reps(repID),
                    FOREIGN KEY (prodID) REFERENCES products(prodID),
                    FOREIGN KEY (custID) REFERENCES customers(custID)
                  );")
}

#///////////////////////////////////////////////////////////////////////////////////////////////////#
# Function to test the database schema
# INPUT: DB Connection
# OUTPUT: Print Statement (If Error)
#///////////////////////////////////////////////////////////////////////////////////////////////////#

test_db_schema <- function(con)
{
  print("Checking Foreign Key Constraints in Sales Table in SQLite DB")
  
  # Insert dummy values into products table
  dbExecute(con, "INSERT INTO products (prodName)
                VALUES 
                    ('Test Product 1'),
                    ('Test Product 2')
          ")
  
  # Insert dummy values into customers table
  dbExecute(con, "INSERT INTO customers (custName, country)
                  VALUES 
                    ('Test Customer 1', 'USA'),
                    ('Test Customer 2', 'USA')
          ")
  
  # Insert dummy values into reps table
  dbExecute(con, "INSERT INTO reps
                  VALUES 
                    (1, 'TestFN1', 'TestSN1', 'Test Territory 1', 1.4),
                    (2, 'TestFN2', 'TestSN2', 'Test Territory 2', 2.4)
          ")
  
  # TEST CASE: Checking Foreign Key Constraints in Sales Table
  # CASE 1: Wrong Reps FK
  tryCatch(
    {
      dbExecute(con, "INSERT INTO sales (txnID, repID, custID, prodID, date, quarter, qty, total, currency)
                      VALUES (1000, 3, 1, 2, '2020-05-13', 'Q2', 20, 500, 'USD')")
    }, 
    error = function(e) {
      print(paste("Error Reps FK:", e$message))
    }
  )
  
  # CASE 2: Wrong Products FK
  tryCatch(
    {
      dbExecute(con, "INSERT INTO sales (txnID, repID, custID, prodID, date, quarter, qty, total, currency)
                      VALUES (1000, 2, 2, 3, '2020-05-13', 'Q2', 20, 500, 'USD')")
    }, 
    error = function(e) {
      print(paste("Error Products FK:", e$message))
    }
  )
  
  # CASE 3: Wrong Customers FK
  tryCatch(
    {
      dbExecute(con, "INSERT INTO sales (txnID, repID, custID, prodID, date, quarter, qty, total, currency)
                      VALUES (1000, 1, 3, 1, '2020-05-13', 'Q2', 20, 500, 'USD')")
    }, 
    error = function(e) {
      print(paste("Error Customers:", e$message))
    }
  )
}

#///////////////////////////////////////////////////////////////////////////////////////////////////#
# Function to extract reps data from XML files with pattern 'pharmaReps*.xml'
# INPUT: XML File Path
# OUTPUT: Dataframe containing all reps data from XML file
#///////////////////////////////////////////////////////////////////////////////////////////////////#

extract_reps_data <- function(xmlFile)
{
  # Parsing XML file into R
  reps_xml <- xmlParse(xmlFile)
  
  # Extract and transform data into data frame
  reps_data <- xmlToDataFrame(nodes = getNodeSet(reps_xml, "//rep"))
  
  # Convert commission column to numeric
  reps_data$commission <- as.numeric(reps_data$commission)
  
  # Extract attribute rID as repID using XPath
  reps_data$repID <- xpathApply(reps_xml, "//rep", xmlGetAttr, "rID")
  
  # Drop the prefix 'r' from repID
  reps_data$repID <- as.integer(substring(reps_data$repID, 2))
  
  # Extract first name and surname from the 'name' node
  # This is done as firstname and surname appear together under the name column, so we extract them separately
  names_data <- xmlToDataFrame(nodes = getNodeSet(reps_xml, "//rep/name"))
  
  # Add firstname column from names_data to reps_data
  reps_data$firstname <- names_data$first
  
  # Add surname column from names_data to reps_data
  reps_data$surname <- names_data$sur
  
  # Drop the 'name' column from reps_data
  reps_data <- subset(reps_data, select = -c(name))
  
  #Delete the names_data data frame
  rm(names_data)

  return(reps_data)
  
}

#///////////////////////////////////////////////////////////////////////////////////////////////////#
# Function to extract sales transaction data from XML files with pattern 'pharmaSalesTxn*.xml'
# INPUT: XML File Path
# OUTPUT: Dataframe containing all sales data from XML file
#///////////////////////////////////////////////////////////////////////////////////////////////////#

extract_txn_data <- function(xmlFile)
{
  # Parsing XML file into R
  txn_xml <- xmlParse(xmlFile)
  
  # Extract and transform data into data frame
  txn_data <- xmlToDataFrame(nodes = getNodeSet(txn_xml, "//txn"))
  
  # Extract attribute txnID using XPath
  txn_data$txnID <- xpathApply(txn_xml, "//txn", xmlGetAttr, "txnID")
  
  # Convert txnID to integer
  txn_data$txnID <- as.integer(txn_data$txnID)
  
  # Extract attribute repID using XPath
  txn_data$repID <- xpathApply(txn_xml, "//txn", xmlGetAttr, "repID")
  
  # Convert repID to integer
  txn_data$repID <- as.integer(txn_data$repID)
  
  # Drop the 'sale' column from txn_data
  txn_data <- subset(txn_data, select = -c(sale))
  
  # Extract date, product, qty and total from the 'sale' node
  # This is done as date, product, qty and total appear together under the name column, so we extract them separately
  sales_data <- xmlToDataFrame(nodes = getNodeSet(txn_xml, "//txn/sale"))
  
  # Extract attribute currency using XPath
  sales_data$currency <- xpathApply(txn_xml, "//txn/sale/total", xmlGetAttr, "currency")
  
  # Unlisting currency into vector
  # This is done for simplicity and to avoid errors later on when manipulating the column as it is a nested attribute
  sales_data$currency <- unlist(sales_data$currency)
  
  # Convert date column to YYYY-MM-DD format
  # Convert date column to character data type to store properly in SQLite database
  sales_data$date <- as.character(as.Date(sales_data$date, format = "%m/%d/%Y"))
  
  # Calculate quarter based on the month of date column
  sales_data$quarter <- ifelse(substring(sales_data$date, 6, 7) %in% c("01", "02", "03"), "Q1",
                        ifelse(substring(sales_data$date, 6, 7) %in% c("04", "05", "06"), "Q2",
                        ifelse(substring(sales_data$date, 6, 7) %in% c("07", "08", "09"), "Q3", "Q4")))
  
  # Convert qty column to integer
  sales_data$qty <- as.integer(sales_data$qty)
  
  # Convert total column to integer
  sales_data$total <- as.integer(sales_data$total)
  
  # Add columns from sales_data to txn_data
  txn_data[c("date", "quarter", "product", "qty", "total", "currency")] <- sales_data[c("date", "quarter", "product", "qty", "total", "currency")]
  
  # Delete sales_data data frame
  rm(sales_data)
  
  return(txn_data)
}

#///////////////////////////////////////////////////////////////////////////////////////////////////#
# Function to load data into reps table in SQLite DB
# INPUT: Connection to SQLite DB, data frame containing data from 'pharmaReps*.xml'
# OUTPUT: None
#///////////////////////////////////////////////////////////////////////////////////////////////////#

load_reps <- function(con, df)
{
  # Load existing repIDs in reps table into data frame
  reps <- dbGetQuery(con, "SELECT repID FROM reps;")
  
  # Drop any rows in data frame if their repID already exists in reps
  df <- df[!(df$repID %in% reps$repID), , drop = FALSE]
  
  # Delete reps data frame
  rm(reps)
  
  # Write data frame into reps table
  dbWriteTable(con, "reps", df, row.names = FALSE, append = TRUE)
}

#///////////////////////////////////////////////////////////////////////////////////////////////////#
# Function to load data into products table in SQLite DB
# INPUT: Connection to SQLite DB, data frame containing data from 'pharmaSalesTxn*.xml'
# OUTPUT: None
#///////////////////////////////////////////////////////////////////////////////////////////////////#

load_products <- function(con, df)
{
  # Load existing product names in products table into data frame
  products <- dbGetQuery(con, "SELECT prodName FROM products;")
  
  # Subset unique product names from data frame
  prod_df <- data.frame(prodName = unique(df$product))
  
  # Drop any rows in data frame if their product name already exists in products 
  prod_df <- prod_df[!(prod_df$prodName %in% products$prodName), , drop = FALSE]
  
  # Delete products data frame
  rm(products)
  
  # Write data frame into products table
  dbWriteTable(con, "products", prod_df, row.names = FALSE, append = TRUE)
}

#///////////////////////////////////////////////////////////////////////////////////////////////////#
# Function to load data into customers table in SQLite DB
# INPUT: Connection to SQLite DB, data frame containing data from 'pharmaSalesTxn*.xml'
# OUTPUT: None
#///////////////////////////////////////////////////////////////////////////////////////////////////#

load_customers <- function(con, df)
{
  # Load existing customer names in customers table into data frame
  customers <- dbGetQuery(con, "SELECT custName, country FROM customers;")
  
  # Extract customer names and countries from data frame
  cust_df <- data.frame(custName = df$customer, country = df$country)
  
  # Subset unique customer name and country combinations from data frame
  cust_df <- unique(cust_df[c("custName", "country")])
  
  # Drop any rows in data frame if their customer name and country combination already exists in customers 
  cust_df <- cust_df[!(cust_df$custName %in% customers$custName & cust_df$country %in% customers$country), , drop = FALSE]
  
  # Delete customers data frame
  rm(customers)
  
  # Write data frame into customers table
  dbWriteTable(con, "customers", cust_df, row.names = FALSE, append = TRUE)
}

#///////////////////////////////////////////////////////////////////////////////////////////////////#
# Function to load data into sales table in SQLite DB
# INPUT: Connection to SQLite DB, data frame containing data from 'pharmaSalesTxn*.xml'
# OUTPUT: None
#///////////////////////////////////////////////////////////////////////////////////////////////////#

load_sales <- function(con, df)
{
  # Load existing rows in products table to data frame
  products <- dbGetQuery(con, "SELECT * FROM products;")
  
  # Load existing rows in customers table to data frame
  customers <- dbGetQuery(con, "SELECT * FROM customers;")
  
  # Create empty vector to store matching FKs from products table
  product_ids <- integer(nrow(df))
  
  # Create empty vector to store matching FKs from customers table
  customer_ids <- integer(nrow(df))
  
  # Iterate over every row in data frame
  for (i in 1:nrow(df))
  {
    
    # Find index in products data frame which matches the product name information for current row
    prod_match <- which(products$prodName == df$product[i])
    
    # Find index in customers data frame which matches the customer name and country information for current row
    cust_match <- which(customers$custName == df$customer[i] & customers$country == df$country[i])
    
    # Check if match exists for product name (If length of match index is 0 then no match is found)
    if (length(prod_match) > 0)
    {
      # If match exists: Store corresponding prodID in vector
      product_ids[i] <- products$prodID[prod_match]
    }
    else
    {
      # If match doesn't exist: Store NA value in vector
      product_ids[i] <- NA
    }
    
    # Check if match exists for customer name and country (If length of match index is 0 then no match is found)
    if (length(cust_match) > 0)
    {
      # If match exists: Store corresponding custID in vector
      customer_ids[i] <- customers$custID[cust_match]
    }
    else
    {
      # If match doesn't exist: Store NA value in vector
      customer_ids[i] <- NA
    }
  }
  
  # Delete products data frame
  rm(products)
  
  # Delete customers data frame
  rm(customers)
  
  # Add product_ids to prodID column in data frame
  df$prodID <- product_ids
  
  # Add customer_ids to custID column in data frame
  df$custID <- customer_ids
  
  # Subset required columns from data frame and re-order according the reps table schema
  df <- df[, c("txnID", "repID", "custID", "prodID", "date", "quarter", "qty", "total", "currency")]

  # Write data frame into sales table
  dbWriteTable(con, "sales", df, row.names = FALSE, append = TRUE)
}

#///////////////////////////////////////////////////////////////////////////////////////////////////#

main <- function() 
{
  # Connect to SQLite DB
  con <- dbConnect(SQLite(), "pharma.db")
  
  # Check the connection
  print("Displaying SQLite DB connection info:")
  print(dbGetInfo(con))
  cat('\n\n')
  
  # Delete all tables in DB; Comment if running script for new set of files
  delete_tables(con)
  
  # Create all tables in DB; Comment if running script for new set of files
  create_tables(con)
  
  # # Comment out block if running script for new set of files
  # # Test DB Schema
  # test_db_schema(con)
  # 
  # # Delete all tables in DB; Comment if running script for new set of files
  # delete_tables(con)
  # 
  # # Create all tables in DB; Comment if running script for new set of files
  # create_tables(con)
  
  # # Add XML file to variable: Testing Code, Ignore
  # xml_file1 <- "txn-xml/pharmaReps-F23.xml"
  # xml_file2 <- "txn-xml/pharmaSalesTxn-10-F23.xml"
  # xml_file3 <- "txn-xml/pharmaSalesTxn-20-F23.xml"
  
  # Store list of all files in txn-xml folder
  xml_folder <- "txn-xml"
  xml_files <- list.files(xml_folder, full.names = TRUE)
  
  # # Filter files starting with "pharmaReps"
  # pharmaReps_xml <- xml_files[grepl("^pharmaReps.*\\.xml$", xml_files)]
  # print(pharmaReps_xml)
  
  # Filter files starting with "pharmaSalesTxn"
  # pharmaSalesTxn_xml <- xml_files[grepl("^pharmaSalesTxn.*\\.xml$", xml_files)]
  # print(pharmaSalesTxn_xml)

  # Iterate through all file names in xml_files
  for (file in xml_files)
  {
    # Check if base file name follows pattern 'pharmaReps*.xml'
    if (grepl("^pharmaReps", basename(file)))
    {
      # Extract data from XML file into data frame
      reps_data <- extract_reps_data(file)
      
      # tryCatch block to handle errors while loading into reps table 
      tryCatch({
        # Begin transaction
        dbExecute(con, "BEGIN TRANSACTION;")
        
        # Load extracted data frame into reps table
        load_reps(con, reps_data) 
        
        # Commit transaction
        dbExecute(con, "COMMIT TRANSACTION;")
        
      }, error = function(e) {
        # Print Message if error occurs while loading
        print(paste("Error loading reps data from file", basename(file)))
        print(e)
        
        # Rollback transaction
        dbExecute(con, "ROLLBACK TRANSACTION;") 
      })
      
    }
    # Check if base file name follows pattern 'pharmaSalesTxn*.xml'
    else if (grepl("^pharmaSalesTxn", basename(file)))
    {
      # Extract data from XML file into data frame
      txn_data <- extract_txn_data(file)
      
      # tryCatch block to handle errors while loading into products, customers, and sales tables
      tryCatch({
        # Begin transaction
        dbExecute(con, "BEGIN TRANSACTION;")
        
        # Load extracted data frame into products table
        load_products(con, txn_data)
        
        # Load extracted data frame into customers table
        load_customers(con, txn_data)
        
        # Load extracted data frame into sales table
        load_sales(con, txn_data)
        
        # Commit transaction
        dbExecute(con, "COMMIT TRANSACTION;")
        
      }, error = function(e) {
        # Print Message if error occurs while loading
        print(paste("Error loading products, customers and sales data from file", basename(file)))
        print(e)
        
        # Rollback transaction
        dbExecute(con, "ROLLBACK TRANSACTION;")
      })
      
    }
      
  }
  
  cat('\n\n')
  
  # Test Code to see the number of rows that have been imported into all tables in DB
  print("Printing number of rows in each table in SQLite DB")
  
  reps <- dbGetQuery(con, "SELECT COUNT(*) AS count FROM reps;")
  print(paste0("Number of rows in reps table: ", reps$count))

  products <- dbGetQuery(con, "SELECT COUNT(*) AS count FROM products;")
  print(paste0("Number of rows in products table: ", products$count))

  customers <- dbGetQuery(con, "SELECT COUNT(*) AS count FROM customers;")
  print(paste0("Number of rows in customers table: ", customers$count))

  sales <- dbGetQuery(con, "SELECT COUNT(*) AS count FROM sales;")
  print(paste0("Number of rows in sales table: ", sales$count))
  
  # Disconnect from SQLite DB
  dbDisconnect(con)
  
}

#///////////////////////////////////////////////////////////////////////////////////////////////////#

# Call main function
main()
