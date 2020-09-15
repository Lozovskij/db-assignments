'use strict';

/********************************************************************************************
 *                                                                                          *
 * The goal of the task is to get basic knowledge of mongodb functions and                  *
 * approaches to work with data in mongodb. Most of the queries should be implemented with  *
 * aggregation pipelines.                                                                   *
 * https://docs.mongodb.com/manual/reference/aggregation/                                   *
 * https://docs.mongodb.com/manual/reference/operator/aggregation/                          *
 *                                                                                          *
 * The course do not includes basic syntax explanations                                     *
 * Start from scratch can be complex for the tasks, if it's the case, please check          *
 * "MongoDB University". The M001 course starts quite often so you can finish it to get     *
 * the basic understanding.                                                                 *
 * https://university.mongodb.com/courses/M001/about                                        *
 *                                                                                          *
 ********************************************************************************************/

/**
 * The function is to add indexes to optimize your queries.
 * Test timeout is increased to 15sec for the function.
 * */
async function before(db) {
    await db.collection('employees').ensureIndex({CustomerID: 1});
    await db.collection('orders').ensureIndex({OrderID: 1});
    await db.collection('orders').ensureIndex({CustomerID: 1});
    await db.collection('customers').ensureIndex({CompanyName: 1});
    await db.collection('customers').ensureIndex({CustomerID: 1});
    await db.collection('products').ensureIndex({ProductName: 1});
    await db.collection('order-details').ensureIndex({OrderID: 1});
}

/**
 *  Create a query to return next data ordered by city and then by name:
 * | Employy Id | Employee Full Name | Title | City |
 *
 * NOTES: if City is null - show city as "Unspecified"
 */
async function task_1_1(db) {
    // The first task is example, please follow the style in the next functions.
    const result = await db.collection('employees').aggregate([
        {
            $project: {
                _id: 0,
                EmployeeID: 1,
                "Employee Full Name": {$concat: ["$FirstName", " ", "$LastName"]},
                Title: 1,
                City: {$ifNull: ['$City', "Unspecified"]}
            }
        },
        {$sort: {City: 1, "Employee Full Name": 1}}
    ]).toArray();
    return result;
}

/**
 *  Create a query to return an Order list ordered by order id descending:
 * | Order Id | Order Total Price | Total Order Discount, % |
 *
 * NOTES:
 *  - Discount in OrderDetails is a discount($) per Unit.
 *  - Round all values to MAX 3 decimal places
 */
async function task_1_2(db) {
    const result = await db.collection('orders').aggregate([
        {
            $lookup:
            {
                from: "order-details",
                localField: "OrderID",
                foreignField: "OrderID",
                as: "purchases"
            }
        },
        {   $unwind:"$purchases" },
        {	
            $group:
            {
                _id: "$OrderID",
                total_price : {$sum: { $multiply: [ "$purchases.UnitPrice", "$purchases.Quantity" ]}},
                sum1 : {$sum: { $multiply: ["$purchases.Discount", "$purchases.Quantity" ]}},
                sum2 : {$sum: { $multiply: [ "$purchases.UnitPrice", "$purchases.Quantity" ]}}
            }
        },
        {
            $project:
            {
                _id: 0,
                "Order Id": "$_id",
                "Order Total Price": {$round : ["$total_price", 3]},
                "Total Order Discount, %": {$round : [ { $multiply: [ {$divide: [ "$sum1", "$sum2"]}, 100]}, 3]}
                
            }
        },
        {	$sort: {"Order Id": -1}	}
    ]).toArray();
    return result;
}

/**
 *  Create a query to return all customers without Fax, order by CustomerID:
 * | CustomerID | CompanyName |
 *
 * HINT: check by string "NULL" values
 */
async function task_1_3(db) {
    const result = await db.collection('customers').aggregate([
        {   $match: {Fax: "NULL"}},
        {
            $project:
            {
                _id: 0,
                CustomerID: "$CustomerID",
                CompanyName: "$CompanyName"
            }
        },
        {	$sort: {"CustomerID": 1}	}
    ]).toArray();
    return result;
}

/**
 * Create a query to return:
 * | Customer Id | Total number of Orders | % of all orders |
 *
 * Order data by % - higher percent at the top, then by CustomerID asc.
 * Round all values to MAX 3 decimal places.
 *
 * HINT: that can done in 2 queries to mongodb.
 * 
 */
async function task_1_4(db) {

    var ordersCount = await db.collection('orders').count().then(result => result);

    const result = await db.collection('orders').aggregate([
        {
            $lookup:
            {
                from: "customers",
                localField: "CustomerID",
                foreignField: "CustomerID",
                as: "buyers"
            }
        },
        {   $unwind:"$buyers" },
        {
            $group:
            {
                _id: "$CustomerID",
                count : {$sum: 1}
            }
        },
        {
            $project:
            {
                _id: 0,
                "Customer Id": "$_id",
                "Total number of Orders": "$count",
                "% of all orders" : {$round :[{$multiply : [ {$divide : ["$count", ordersCount]}, 100 ]},3]}
            }
        },
        {	$sort: {"% of all orders" : -1, "Customer Id" : 1}	}
    ]).toArray();
    return result;
}

/**
 * Return all products where product name starts with 'A', 'B', .... 'F' ordered by name.
 * | ProductID | ProductName | QuantityPerUnit |
 */
async function task_1_5(db) {
    const result = await db.collection('products').aggregate([
        {
            $match : { ProductName : /^[ABCDEF]/	}
        },
        {
            $project:
            {
                _id: 0,
                ProductID: 1,
                ProductName: 1,
                QuantityPerUnit : 1
            }
        },
        {	$sort: { ProductName : 1}	}
    ]).toArray();
    return result;
}

/**
 *
 * Create a query to return all products with category and supplier company names:
 * | ProductName | CategoryName | SupplierCompanyName |
 *
 * Order by ProductName then by SupplierCompanyName
 *
 * HINT: see $lookup operator
 *       https://docs.mongodb.com/manual/reference/operator/aggregation/lookup/
 */
async function task_1_6(db) {
    const result = await db.collection('products').aggregate([
        {
            $lookup:
            {
                from: "suppliers",
                localField: "SupplierID",
                foreignField: "SupplierID",
                as: "supliersArr"
            }
        },
        {
            $lookup:
            {
                from: "categories",
                localField: "CategoryID",
                foreignField: "CategoryID",
                as: "categoriesArr"
            }
        },
        {   $unwind:"$supliersArr" },
        {   $unwind:"$categoriesArr" },
        {
            $project:
            {
                _id: 0,
                ProductName: 1,
                CategoryName: "$categoriesArr.CategoryName",
                SupplierCompanyName : "$supliersArr.CompanyName"
            }
        },
        {	$sort: { ProductName : 1, CompanyName: 1}	}
    ]).toArray();
    return result;
}

/**
 *
 * Create a query to return all employees and full name of person to whom this employee reports to:
 * | EmployeeID | FullName | ReportsTo |
 *
 * Full Name - title of courtesy with full name.
 * Order data by EmployeeID.
 * Reports To - Full name. If the employee does not report to anybody leave "-" in the column.
 */
async function task_1_7(db) {
    
    const result = await db.collection('employees').aggregate([
        {
            $lookup:
            {
                from: "employees",
                localField: "ReportsTo",
                foreignField: "EmployeeID",
                as: "employeesArr"
            }
        },
        {
            $project:
            {
                _id: 0,
                EmployeeID: 1,
                FullName: {$concat: ["$TitleOfCourtesy", "$FirstName", " ", "$LastName"]},
                ReportsTo: 
                { 
                    $ifNull: [ 
                        {$concat: [{$arrayElemAt: ["$employeesArr.FirstName",0]}, " ", {$arrayElemAt: ["$employeesArr.LastName",0]}]}, 
                        "-"]
                }
            }
        },
        {	$sort: { EmployeeID : 1}	}
    ]).toArray();
    return result;
}

/**
 *
 * Create a query to return:
 * | CategoryName | TotalNumberOfProducts |
 * Order by CategoryName
 */
async function task_1_8(db) {
    const result = await db.collection('categories').aggregate([
        {
            $lookup:
            {
                from: "products",
                localField: "CategoryID",
                foreignField: "CategoryID",
                as: "productsArr"
            }
        },
        {   $unwind:"$productsArr" },
        {
            $group:
            {
                _id: "$CategoryName",
                TotalNumberOfProducts: {$sum : 1}
            } 
        },
        {
            $project:
            {
                _id: 0,
                CategoryName: "$_id",
                TotalNumberOfProducts: "$TotalNumberOfProducts"
            }
        },
        {	$sort: { CategoryName : 1}	}
    ]).toArray();
    return result;
}

/**
 *
 * Create a query to find those customers whose contact name containing the 1st character is 'F' and the 4th character is 'n' and rests may be any character.
 * | CustomerID | ContactName |
 * order by CustomerID
 */
async function task_1_9(db) {
    const result = await db.collection('customers').aggregate([
        {
            $match : { ContactName : /^F..n/	}
        },
        {
            $project:
            {
                _id: 0,
                CustomerID: 1,
                ContactName: 1
            }
        },
        {	$sort: { CustomerID : 1}	}
    ]).toArray();
    return result;
}

/**
 * Write a query to get discontinued Product list:
 * | ProductID | ProductName |
 * order by ProductID
 */
async function task_1_10(db) {
    const result = await db.collection('products').aggregate([
        {
            $match : { Discontinued : 1	}
        },
        {
            $project:
            {
                _id: 0,
                ProductID: 1,
                ProductName: 1
            }
        },
        {	$sort: { ProductID : 1}	}
    ]).toArray();
    return result;
}

/**
 * Create a query to get Product list (name, unit price) where products cost between $5 and $15:
 * | ProductName | UnitPrice |
 *
 * Order by UnitPrice then by ProductName
 */
async function task_1_11(db) {
    const result = await db.collection('products').aggregate([
        {
            $match : { UnitPrice : { $gte: 5, $lte: 15 }	}
        },
        {
            $project:
            {
                _id: 0,
                ProductName: 1,
                UnitPrice: 1
            }
        },
        {	$sort: { UnitPrice : 1, ProductName : 1}	}
    ]).toArray();
    return result;
}

/**
 * Write a SQL query to get Product list of twenty most expensive products:
 * | ProductName | UnitPrice |
 *
 * Order products by price (asc) then by ProductName.
 */
async function task_1_12(db) {
    const result = await db.collection('products').aggregate([
        {
            $project:
            {
                _id: 0,
                ProductName: 1,
                UnitPrice: 1
            }
        },
        {	$sort: { UnitPrice : -1, ProductName : 1}	},
        { 	$limit: 20 	},
        {	$sort: { UnitPrice : 1, ProductName : 1}	}
    ]).toArray();
    return result;
}

/**
 * Create a query to count current and discontinued products:
 * | TotalOfCurrentProducts | TotalOfDiscontinuedProducts |
 *
 * HINT: That's acceptable to make it in 2 queries
 */
async function task_1_13(db) {
    const TotalOfCurrentProducts = await db.collection('products').aggregate([
        {
            $group: 
            {
                _id: "",
                TotalOfCurrentProducts: { $sum: 1 }
            },
        },
        {
            $project:
            {
                _id: 0,
                TotalOfCurrentProducts: "$TotalOfCurrentProducts"
            }
        }
    ]).toArray().then(result => result[0].TotalOfCurrentProducts);

    const result = await db.collection('products').aggregate([
        {
            $match : { Discontinued : 1	}
        },
        {
            $group: 
            {
                _id: "",
                TotalOfDiscontinuedProducts: { $sum: 1 }
            },
        },
        {
            $project:
            {
                _id: 0,
                TotalOfCurrentProducts: {$literal : TotalOfCurrentProducts},
                TotalOfDiscontinuedProducts: "$TotalOfDiscontinuedProducts"
            }
        }
    ]).toArray().then(result => result[0]);  
    return result;
}

/**
 * Create a query to get Product list of stock is less than the quantity on order:
 * | ProductName | UnitsOnOrder| UnitsInStock |
 * Order by ProductName
 *
 * HINT: see $expr operator
 *       https://docs.mongodb.com/manual/reference/operator/query/expr/#op._S_expr
 */
async function task_1_14(db) {
    const result = await db.collection('products').aggregate([
        {
            $match : { $expr: { $gt: [ "$UnitsOnOrder" , "$UnitsInStock" ] } }
        },
        {
            $project:
            {
                _id: 0,
                ProductName: "$ProductName",
                UnitsOnOrder: "$UnitsOnOrder",
                UnitsInStock: "$UnitsInStock"
            }
        },
        {	$sort: {ProductName: 1}	}
    ]).toArray();
    return result;
}

/**
 * Create a query to return the total number of orders for every month in 1997 year:
 * | January | February | March | April | May | June | July | August | September | November | December |
 *
 * HINT: see $dateFromString
 *       https://docs.mongodb.com/manual/reference/operator/aggregation/dateFromString/
 */
async function task_1_15(db) {
    const orderPerMonthArr = await db.collection('orders').aggregate([
        {
            $match: 
            {
                $expr: 
                {
                    $eq: [{$year: {$dateFromString: {dateString: "$OrderDate"}}}, 1997]
                }
            }
        },
        { 
            $group: 
            {
                _id:
                {	
                    $month: { $dateFromString: { dateString: "$OrderDate"}}
                },
                count: { $sum: 1 }
            }
        },
        {
            $project: 
            {
                _id : 0,
                month : "$_id",
                count: "$count"
            }
        },
        {	$sort: {month: 1}	}
    ]).toArray().then(result => result);

    const result = await db.collection('products').aggregate([
        {	$group: { _id: "" }},
        {
            $project: 
            {
                _id : 0,
                January : {$literal : orderPerMonthArr[0].count},
                February: {$literal : orderPerMonthArr[1].count},
                March: {$literal : orderPerMonthArr[2].count},
                April: {$literal : orderPerMonthArr[3].count},
                May: {$literal : orderPerMonthArr[4].count},
                June: {$literal : orderPerMonthArr[5].count},
                July: {$literal : orderPerMonthArr[6].count},
                August: {$literal : orderPerMonthArr[7].count},
                September: {$literal : orderPerMonthArr[8].count},
                October: {$literal : orderPerMonthArr[9].count},
                November: {$literal : orderPerMonthArr[10].count},
                December: {$literal : orderPerMonthArr[11].count}
            }
        }
    ]).toArray().then(result => result[0]);

    return result;
}

/**
 * Create a query to return all orders where ship postal code is provided:
 * | OrderID | CustomerID | ShipCountry |
 * Order by OrderID
 */
async function task_1_16(db) {
   
    const result = await db.collection('orders').aggregate([
        /*{
            $match : { ShipPostalCode: {$ne: "NULL"}}
        },*/
        {
            $project:
            {
                _id: 0,
                OrderID: 1,
                CustomerID: 1,
                ShipCountry: 1
            }
        },
        {	$sort: { OrderID : 1}	}
    ]).toArray();
    return result;
}

/**
 * Create SQL query to display the average price of each categories's products:
 * | CategoryName | AvgPrice |
 * Order by AvgPrice descending then by CategoryName
 * NOTES:
 *  - Round AvgPrice to MAX 2 decimal places
 */
async function task_1_17(db) {
    const result = await db.collection('products').aggregate([
        {
            $lookup:
            {
                from: "categories",
                localField: "CategoryID",
                foreignField: "CategoryID",
                as: "categoriesArr"
            }
        },
        {
            $unwind : "$categoriesArr"
        },
        {
            $group:
            {
                _id : "$categoriesArr.CategoryName",
                AvgPrice : {$avg : "$UnitPrice"}
            }
        },
        {
            $project:
            {
                _id: 0,
                CategoryName : "$_id",
                AvgPrice : { $round : ["$AvgPrice", 2] }
            }
        },
        {	$sort: { AvgPrice: -1, CategoryName : 1}	}
    ]).toArray();
    return result;
}

/**
 * Create a query to calcualte total orders count by each day in 1998:
 * | Order Date | Total Number of Orders |
 *
 * Order Date needs to be in the format '%Y-%m-%d'
 * Order by Order Date
 *
 * HINT: see $dateFromString, $dateToString
 *       https://docs.mongodb.com/manual/reference/operator/aggregation/dateToString/
 *       https://docs.mongodb.com/manual/reference/operator/aggregation/dateFromString/
 */
async function task_1_18(db) {
    const result = await db.collection('orders').aggregate([
        {
            $match: 
            {
                $expr: 
                {
                    $eq: [{$year: {$dateFromString: {dateString: "$OrderDate"}}}, 1998]
                }
            }
        },
        {
            $group:
            {
                _id : { $dateToString: { 
                    date: { $dateFromString: {dateString: "$OrderDate"} }, 
                    format: "%Y-%m-%d"} },
                OrdersPerDay : {$sum : 1}
            }
        },
        {
            $project:
            {
                _id: 0,
                "Order Date" : "$_id",
                "Total Number of Orders" : "$OrdersPerDay"
            }
        },
        {	$sort: { "Order Date" : 1}	}
    ]).toArray();
    return result;
}

/**
 * Create a query to display customer details whose total orders amount is more than 10000$:
 * | CustomerID | CompanyName | TotalOrdersAmount, $ |
 *
 * Order by "TotalOrdersAmount, $" descending then by CustomerID
 *  NOTES:
 *  - Round TotalOrdersAmount to MAX 2 decimal places
 *
 *  HINT: the query can be slow, you need to optimize it and pass in 2 seconds
 *       - https://docs.mongodb.com/manual/tutorial/analyze-query-plan/
 *       - quite often you can solve performance issues just with adding PROJECTIONS.
 *         *** Use Projections to Return Only Necessary Data ***
 *         https://docs.mongodb.com/manual/tutorial/optimize-query-performance-with-indexes-and-projections/#use-projections-to-return-only-necessary-data
 *       - do not hesitate to "ensureIndex" in "before" function at the top if needed https://docs.mongodb.com/manual/reference/method/db.collection.ensureIndex/
 */
async function task_1_19(db) {
    const result = await db.collection('orders').aggregate([
        {
            $lookup: {
                from: "order-details",
                localField: "OrderID",
                foreignField: "OrderID",
                as: "orderDetailsArr"
            }
        },
        {
            $lookup: {
                from: "customers",
                localField: "CustomerID",
                foreignField: "CustomerID",
                as: "customersArr"
            }
        },
        {	$unwind: "$customersArr" },
        {	$unwind: "$orderDetailsArr" },
        {	
            $group: {
                _id : "$customersArr.CustomerID",
                FirstCompanyName : { $first: "$customersArr.CompanyName" },
                buyerExpenses : { $sum: {$multiply:["$orderDetailsArr.UnitPrice", "$orderDetailsArr.Quantity"]}}
            }
        },
        {	$match : {$expr : {$gt : ["$buyerExpenses", 10000]}}},
        {
            $project: {
                _id: 0,
                CustomerID: "$_id",
                CompanyName: "$FirstCompanyName",
                "TotalOrdersAmount, $" : { $round :["$buyerExpenses",2]}
            }
        },
        {	$sort: { "TotalOrdersAmount, $" : -1, CustomerID : 1}	}
    ]).toArray();
    return result;
}

/**
 *
 * Create a query to find the employee that sold products for the largest amount:
 * | EmployeeID | Employee Full Name | Amount, $ |
 */
async function task_1_20(db) {
    const result = await db.collection('orders').aggregate([
        {
            $lookup: {
                from: "order-details",
                localField: "OrderID",
                foreignField: "OrderID",
                as: "orderDetailsArr"
            }
        },
        {
            $lookup: {
                from: "employees",
                localField: "EmployeeID",
                foreignField: "EmployeeID",
                as: "employeesArr"
            }
        },
        {	$unwind: "$employeesArr" },
        {	$unwind: "$orderDetailsArr" },
        {	
            $group: {
                _id : "$employeesArr.EmployeeID",
                FirstName: {$first : "$employeesArr.FirstName"},
                LastName: {$first : "$employeesArr.LastName"},
                Amount : { $sum: {$multiply:["$orderDetailsArr.UnitPrice", "$orderDetailsArr.Quantity"]}}
            }
        },
        {
            $project: {
                _id: 0,
                EmployeeID: "$_id",
                "Employee Full Name": {$concat : ["$FirstName", " ", "$LastName"]},
                "Amount, $" : { $round :["$Amount",2]}
            }
        },
        {	$sort: { "Amount, $" : -1}	},
        {	$limit: 1}
    ]).toArray();
    return result;
}

/**
 * Write a SQL statement to get the maximum purchase amount of all the orders.
 * | OrderID | Maximum Purchase Amount, $ |
 */
async function task_1_21(db) {
    const result = await db.collection('orders').aggregate([
        {
            $lookup: {
                from: "order-details",
                localField: "OrderID",
                foreignField: "OrderID",
                as: "orderDetailsArr"
            }
        },
        {	$unwind: "$orderDetailsArr" },
        {	
            $group: {
                _id : "$orderDetailsArr.OrderID",
                purchaseAmount : { $sum: {$multiply:["$orderDetailsArr.UnitPrice", "$orderDetailsArr.Quantity"]}}
            }
        },
        {
            $project: {
                _id: 0,
                OrderID: "$_id",
                "Maximum Purchase Amount, $" : { $round :["$purchaseAmount",2]}
            }
        },
        {	$sort: { "Maximum Purchase Amount, $" : -1}	},
        {	$limit: 1}
    ]).toArray();
    return result;
}

/**
 * Create a query to display the name of each customer along with their most expensive purchased product:
 * CustomerID | CompanyName | ProductName | PricePerItem |
 *
 * order by PricePerItem descending and them by CompanyName and ProductName acceding
 *
 * HINT: you can use pipeline inside of #lookup
 *       https://docs.mongodb.com/manual/reference/operator/aggregation/lookup/#join-conditions-and-uncorrelated-sub-queries
 */
async function task_1_22(db) {
    const result = await db.collection('orders').aggregate([
        {	
            $project:{
                _id: 0,
                OrderID : 1,
                CustomerID : 1
            }
        },
        {
            $lookup: {
                from: "customers",
                localField: "CustomerID",
                foreignField: "CustomerID",
                as: "customersArr"
            }
        },
        {	$unwind: "$customersArr" },
        {	
            $project:{
                _id: 0,
                OrderID : 1,
                CustomerID : "$customersArr.CustomerID",
                CompanyName : "$customersArr.CompanyName"
            }
        },
        {
            $lookup: {
                from: "order-details",
                localField: "OrderID",
                foreignField: "OrderID",
                as: "detailsArr"
            }
        },
        {	$unwind: "$detailsArr" },
        {	
            $project:{
                _id: 0,
                CustomerID : 1,
                CompanyName : 1,
    
                UnitPrice : "$detailsArr.UnitPrice",
                ProductID : "$detailsArr.ProductID"
            }
        },
        {
            $lookup: {
                from: "products",
                localField: "ProductID",
                foreignField: "ProductID",
                as: "productsArr"
            }
        },
        {	$unwind: "$productsArr" },
        {	
            $project:{
                _id: 0,
                CustomerID : 1,
                CompanyName : 1,
                UnitPrice : 1,
                ProductName : "$productsArr.ProductName"
            }
        },
        { 	$sort: { "UnitPrice": -1 } },
        {
            $group: {
                _id: "$CustomerID", 
                firstProduct: { $first: "$ProductName" },
                firstCompName: { $first: "$CompanyName" },
                maxUnitPrice: { $max: "$UnitPrice" }
            }
        },
        {
            $project: {
                _id: 0,
                CustomerID: "$_id",
                CompanyName: "$firstCompName",
                ProductName: "$firstProduct",
                PricePerItem: "$maxUnitPrice"
            }
        },
        { $sort: { PricePerItem: -1, CompanyName: 1, ProductName: 1 } }
    ]).toArray();
    return result;
}

module.exports = {
    before: before,
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
