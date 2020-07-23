'use strict';

/********************************************************************************************
 *                                                                                          *
 * The goal of the task is to get basic knowledge of SQL functions and                      *
 * approaches to work with data in SQL.                                                     *
 * https://dev.mysql.com/doc/refman/5.7/en/function-reference.html                          *
 *                                                                                          *
 * The course do not includes basic syntax explanations. If you see the SQL first time,     *
 * you can find explanation and some trainings at W3S                                       *
 * https://www.w3schools.com/sql/sql_syntax.asp                                             *
 *                                                                                          *
 ********************************************************************************************/


/**
 *  Create a SQL query to return next data ordered by city and then by name:
 * | Employy Id | Employee Full Name | Title | City |
 *
 * @return {array}
 *
 */
async function task_1_1(db) {
    // The first task is example, please follow the style in the next functions.
    let result = await db.query(`
        SELECT
           EmployeeID as "Employee Id",
           CONCAT(FirstName, ' ', LastName) AS "Employee Full Name",
           Title as "Title",
           City as "City"
        FROM Employees
        ORDER BY City, "Employee Full Name"
    `);
    return result[0];
}

/**
 *  Create a query to return an Order list ordered by order id descending:
 * | Order Id | Order Total Price | Total Order Discount, % |
 *
 * NOTES: Discount in OrderDetails is a discount($) per Unit.
 * @return {array}
 *
 */
async function task_1_2(db) {
    let result = await db.query(`
    SELECT 
        OrderID as "Order Id", 
        SUM(Quantity * UnitPrice) as "Order Total Price",
        ROUND( SUM(Discount * Quantity) / SUM(UnitPrice * Quantity) * 100, 3) as "Total Order Discount, %"
    FROM northwind.OrderDetails
    GROUP BY \`Order Id\`
    ORDER BY \`Order Id\` DESC;
    `);
    return result[0];
}

/**
 *  Create a query to return all customers from USA without Fax:
 * | CustomerId | CompanyName |
 *
 * @return {array}
 *
 */
async function task_1_3(db) {
    let result = await db.query(`
    SELECT CustomerID as CustomerId, CompanyName 
    FROM northwind.Customers
    WHERE Country LIKE "USA"
        AND Fax IS NULL;
    `);
    return result[0];
}

/**
 * Create a query to return:
 * | Customer Id | Total number of Orders | % of all orders |
 *
 * order data by % - higher percent at the top, then by CustomerID asc
 *
 * @return {array}
 *
 */
async function task_1_4(db) {
    let result = await db.query(`
    SELECT 
        CustomerID as "Customer Id", 
        COUNT(OrderDate) as "Total number of Orders",
        ROUND(COUNT(OrderDate) / (SELECT COUNT(*) FROM northwind.Orders) * 100, 5) as "% of all orders"
    FROM northwind.Orders
    GROUP BY CustomerID
    ORDER BY \`% of all orders\` DESC, CustomerID
    `);
    return result[0];
}

/**
 * Return all products where product name starts with 'A', 'B', .... 'F' ordered by name.
 * | ProductId | ProductName | QuantityPerUnit |
 *
 * @return {array}
 *
 */
async function task_1_5(db) {
    let result = await db.query(`
    SELECT ProductID as ProductId, ProductName, QuantityPerUnit 
    FROM northwind.Products
    WHERE Substring(ProductName, 1, 1) IN ("A","B","C","D","E","F")
    ORDER BY ProductName;
    `);
    return result[0];
}

/**
 *
 * Create a query to return all products with category and supplier company names:
 * | ProductName | CategoryName | SupplierCompanyName |
 *
 * Order by ProductName then by SupplierCompanyName
 * @return {array}
 *
 */
async function task_1_6(db) {
    let result = await db.query(`
    SELECT ProductName, CategoryName, CompanyName as SupplierCompanyName
    FROM Products
        INNER JOIN Categories on Products.CategoryID = Categories.CategoryID
        INNER JOIN Suppliers on Products.SupplierID = Suppliers.SupplierID
    ORDER BY ProductName, SupplierCompanyName;
    `); 
    return result[0];
}

/**
 *
 * Create a query to return all employees and full name of person to whom this employee reports to:
 * | EmployeeId | FullName | ReportsTo |
 *
 * Full Name - title of courtesy with full name.
 * Order data by EmployeeId.
 * Reports To - Full name. If the employee does not report to anybody leave "-" in the column.
 * @return {array}
 *
 */
async function task_1_7(db) {
    let result = await db.query(`
    SELECT 
        table_1.EmployeeID as EmployeeId, 
        CONCAT(table_1.FirstName, ' ', table_1.LastName) as FullName,
        CASE 
            WHEN table_1.ReportsTo IS NULL THEN "-"
            ELSE CONCAT(table_2.FirstName, ' ', table_2.LastName)
        END AS ReportsTo
    FROM Employees as table_1
        LEFT JOIN Employees as table_2 on table_1.ReportsTo = table_2.EmployeeID;
    `); 
    return result[0];
}

/**
 *
 * Create a query to return:
 * | CategoryName | TotalNumberOfProducts |
 *
 * @return {array}
 *
 */
async function task_1_8(db) {
    let result = await db.query(`
    SELECT 
        CategoryName, 
        COUNT(CategoryName) as TotalNumberOfProducts 
    FROM northwind.Products
        INNER JOIN Categories on Products.CategoryID = Categories.CategoryID
    GROUP BY CategoryName
    ORDER BY CategoryName;
    `); 
    return result[0];
}

/**
 *
 * Create a SQL query to find those customers whose contact name containing the 1st character is 'F' and the 4th character is 'n' and rests may be any character.
 * | CustomerID | ContactName |
 *
 * @return {array}
 *
 */
async function task_1_9(db) {
    let result = await db.query(`
    SELECT CustomerID, ContactName 
    FROM northwind.Customers
    WHERE ContactName LIKE "F__n%";
    `); 
    return result[0];
}

/**
 * Write a query to get discontinued Product list:
 * | ProductID | ProductName |
 *
 * @return {array}
 *
 */
async function task_1_10(db) {
    let result = await db.query(`
    SELECT ProductID,ProductName 
    FROM northwind.Products
    WHERE Discontinued = 1;
    `); 
    return result[0];
}

/**
 * Create a SQL query to get Product list (name, unit price) where products cost between $5 and $15:
 * | ProductName | UnitPrice |
 *
 * Order by UnitPrice then by ProductName
 *
 * @return {array}
 *
 */
async function task_1_11(db) {
    let result = await db.query(`
    SELECT ProductName, UnitPrice 
    FROM northwind.Products
    WHERE UnitPrice >= 5 AND UnitPrice <= 15
    ORDER BY UnitPrice, ProductName;	
    `); 
    return result[0];
}

/**
 * Write a SQL query to get Product list of twenty most expensive products:
 * | ProductName | UnitPrice |
 *
 * Order products by price then by ProductName.
 *
 * @return {array}
 *
 */
async function task_1_12(db) {
    let result = await db.query(`
    SELECT * FROM
        (SELECT ProductName, UnitPrice 
        FROM northwind.Products
        ORDER BY UnitPrice desc
        LIMIT 20) AS T
    ORDER BY UnitPrice, ProductName;
    `); 
    return result[0];
}


/**
 * Create a SQL query to count current and discontinued products:
 * | TotalOfCurrentProducts | TotalOfDiscontinuedProducts |
 *
 * @return {array}
 *
 */
async function task_1_13(db) {
    let result = await db.query(`
    SELECT
        COUNT(Discontinued) as TotalOfCurrentProducts,
        SUM(CASE Discontinued WHEN 1 THEN 1 ELSE 0 END) as TotalOfDiscontinuedProducts
    FROM northwind.Products;
    `); 
    return result[0];
}

/**
 * Create a SQL query to get Product list of stock is less than the quantity on order:
 * | ProductName | UnitsOnOrder| UnitsInStock |
 *
 * @return {array}
 *
 */
async function task_1_14(db) {
    let result = await db.query(`
    SELECT ProductName, UnitsOnOrder, UnitsInStock FROM northwind.Products
    WHERE UnitsInStock < UnitsOnOrder;
    `); 
    return result[0];
}

/**
 * Create a SQL query to return the total number of orders for every month in 1997 year:
 * | January | February | March | April | May | June | July | August | September | November | December |
 *
 * @return {array}
 *
 */
async function task_1_15(db) {
    let result = await db.query(`
    SELECT 
    SUM(CASE YEAR(OrderDate) WHEN 1997 THEN (CASE month(OrderDate) WHEN 1 THEN 1 ELSE 0 END) ELSE 0 END) as January,
    SUM(CASE YEAR(OrderDate) WHEN 1997 THEN (CASE month(OrderDate) WHEN 2 THEN 1 ELSE 0 END) ELSE 0 END) as February,
    SUM(CASE YEAR(OrderDate) WHEN 1997 THEN (CASE month(OrderDate) WHEN 3 THEN 1 ELSE 0 END) ELSE 0 END) as March,
    SUM(CASE YEAR(OrderDate) WHEN 1997 THEN (CASE month(OrderDate) WHEN 4 THEN 1 ELSE 0 END) ELSE 0 END) as April,
    SUM(CASE YEAR(OrderDate) WHEN 1997 THEN (CASE month(OrderDate) WHEN 5 THEN 1 ELSE 0 END) ELSE 0 END) as May,
    SUM(CASE YEAR(OrderDate) WHEN 1997 THEN (CASE month(OrderDate) WHEN 6 THEN 1 ELSE 0 END) ELSE 0 END) as June,
    SUM(CASE YEAR(OrderDate) WHEN 1997 THEN (CASE month(OrderDate) WHEN 7 THEN 1 ELSE 0 END) ELSE 0 END) as July,
    SUM(CASE YEAR(OrderDate) WHEN 1997 THEN (CASE month(OrderDate) WHEN 8 THEN 1 ELSE 0 END) ELSE 0 END) as August,
    SUM(CASE YEAR(OrderDate) WHEN 1997 THEN (CASE month(OrderDate) WHEN 9 THEN 1 ELSE 0 END) ELSE 0 END) as September,
    SUM(CASE YEAR(OrderDate) WHEN 1997 THEN (CASE month(OrderDate) WHEN 10 THEN 1 ELSE 0 END) ELSE 0 END) as October,
    SUM(CASE YEAR(OrderDate) WHEN 1997 THEN (CASE month(OrderDate) WHEN 11 THEN 1 ELSE 0 END) ELSE 0 END) as November,
    SUM(CASE YEAR(OrderDate) WHEN 1997 THEN (CASE month(OrderDate) WHEN 12 THEN 1 ELSE 0 END) ELSE 0 END) as December
    FROM northwind.Orders;
    `); 
    return result[0];
}

/**
 * Create a SQL query to return all orders where ship postal code is provided:
 * | OrderID | CustomerID | ShipCountry |
 *
 * @return {array}
 *
 */
async function task_1_16(db) {
    let result = await db.query(`
    SELECT OrderID, CustomerID, ShipCountry 
    FROM northwind.Orders
    WHERE ShipPostalCode IS NOT NULL;
    `); 
    return result[0];
}

/**
 * Create SQL query to display the average price of each categories's products:
 * | CategoryName | AvgPrice |
 *
 * @return {array}
 *
 * Order by AvgPrice descending then by CategoryName
 *
 */
async function task_1_17(db) {
    let result = await db.query(`
    SELECT 
        CategoryName, 
        AVG(UnitPrice) as AvgPrice
    FROM northwind.Products
        INNER JOIN Categories on products.CategoryID = Categories.CategoryID
    GROUP BY CategoryName
    ORDER BY AvgPrice DESC, CategoryName
    `); 
    return result[0];
}

/**
 * Create a SQL query to calcualte total orders count by each day in 1998:
 * | OrderDate | Total Number of Orders |
 *
 * Order Date needs to be in the format '%Y-%m-%d %T'
 * @return {array}
 *
 */
async function task_1_18(db) {
    let result = await db.query(`
    SELECT * 
    FROM (SELECT 
            date_format(OrderDate, '%Y-%m-%d %T') as OrderDate, 
            COUNT(OrderID) as "Total Number of Orders" 
            FROM northwind.Orders 
            GROUP BY OrderDate) AS T
    WHERE YEAR(T.OrderDate) = 1998;
    `); 
    return result[0];
}

/**
 * Create a SQL query to display customer details whose total orders amount is more than 10000$:
 * | CustomerID | CompanyName | TotalOrdersAmount, $ |
 *
 * Order by "TotalOrdersAmount, $" descending then by CustomerID
 * @return {array}
 *
 */
async function task_1_19(db) {
    let result = await db.query(`
    SELECT 
        Orders.CustomerID, 
        Customers.CompanyName,
        SUM(OrderDetails.UnitPrice * OrderDetails.Quantity) as "TotalOrdersAmount, $"
    FROM northwind.Orders
        INNER JOIN Customers on Orders.CustomerID = Customers.CustomerID
        INNER JOIN OrderDetails on Orders.OrderID = OrderDetails.OrderID
    GROUP BY Orders.CustomerID
    HAVING \`TotalOrdersAmount, $\` > 10000
    ORDER BY \`TotalOrdersAmount, $\` DESC, Orders.CustomerID;
    `); 
    return result[0];
}

/**
 *
 * Create a SQL query to find the employee that sold products for the largest amount:
 * | EmployeeID | Employee Full Name | Amount, $ |
 *
 * @return {array}
 *
 */
async function task_1_20(db) {
    let result = await db.query(`
    SELECT 
        Employees.EmployeeID, 
        CONCAT(Employees.FirstName, ' ', Employees.LastName) as "Employee Full Name",
        SUM(OrderDetails.UnitPrice * OrderDetails.Quantity) as "Amount, $"
    FROM northwind.Orders
        INNER JOIN Employees on Orders.EmployeeID = Employees.EmployeeID
        INNER JOIN OrderDetails on Orders.OrderID = OrderDetails.OrderID
    GROUP BY EmployeeID
    ORDER BY \`Amount, $\` DESC
    LIMIT 1;
    `); 
    return result[0];
}

/**
 * Write a SQL statement to get the maximum purchase amount of all the orders.
 * | OrderID | Maximum Purchase Amount, $ |
 *
 * @return {array}
 */
async function task_1_21(db) {
    let result = await db.query(`
    SELECT 
        Orders.OrderID,
        SUM(OrderDetails.UnitPrice * OrderDetails.Quantity) as "Maximum Purchase Amount, $"
    FROM northwind.Orders
        INNER JOIN OrderDetails on Orders.OrderID = OrderDetails.OrderID
    GROUP BY OrderDetails.OrderID
    ORDER BY \`Maximum Purchase Amount, $\` DESC
    LIMIT 1;
    `); 
    return result[0];
}

/**
 * Create a SQL query to display the name of each customer along with their most expensive purchased product:
 * | CompanyName | ProductName | PricePerItem |
 *
 * order by PricePerItem descending and them by CompanyName and ProductName acceding
 * @return {array}
 */
async function task_1_22(db) {
    let result = await db.query(`
    SELECT t1.CompanyName, t1.ProductName, t1.PricePerItem as PricePerItem
    FROM (
        SELECT 
            Customers.CompanyName as CompanyName,
            Products.ProductName as ProductName,
            OrderDetails.UnitPrice as PricePerItem,
            Orders.CustomerID as CustomerID
        FROM northwind.Orders
            INNER JOIN OrderDetails on Orders.OrderID = OrderDetails.OrderID
            INNER JOIN Customers on Orders.CustomerID = Customers.CustomerID
            INNER JOIN Products on OrderDetails.ProductID = Products.ProductID
        ) as t1 
        LEFT JOIN
        (
        SELECT 
            Customers.CompanyName as CompanyName,
            Products.ProductName as ProductName,
            OrderDetails.UnitPrice as PricePerItem,
            Orders.CustomerID as CustomerID
        FROM northwind.Orders
            INNER JOIN OrderDetails on Orders.OrderID = OrderDetails.OrderID
            INNER JOIN Customers on Orders.CustomerID = Customers.CustomerID
            INNER JOIN Products on OrderDetails.ProductID = Products.ProductID
        ) as t2 /*t2 is copy of t1*/
        ON t1.CustomerID = t2.CustomerID AND t1.PricePerItem < t2.PricePerItem 
    WHERE t2.PricePerItem IS NULL
    GROUP BY CompanyName, ProductName /*in case there is two products with the same price*/
    ORDER BY t1.PricePerItem DESC, CompanyName, ProductName;
    `); 
    return result[0];
}

module.exports = {
    task_1_1: task_1_1,
    task_1_2: task_1_2,
    task_1_3: task_1_3,
    task_1_4: task_1_4,
    task_1_5: task_1_5,
    task_1_6: task_1_6,
    task_1_7: task_1_7,
    task_1_8: task_1_8,
    task_1_9: task_1_9,
    task_1_10: task_1_10,
    task_1_11: task_1_11,
    task_1_12: task_1_12,
    task_1_13: task_1_13,
    task_1_14: task_1_14,
    task_1_15: task_1_15,
    task_1_16: task_1_16,
    task_1_17: task_1_17,
    task_1_18: task_1_18,
    task_1_19: task_1_19,
    task_1_20: task_1_20,
    task_1_21: task_1_21,
    task_1_22: task_1_22
};
