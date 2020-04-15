DROP TABLE Customers;
CREATE TABLE Customers (Customer_ID integer, Name VARCHAR(200), State VARCHAR(200));
INSERT INTO Customers VALUES (41, 'Matt', 'VA');
INSERT INTO Customers VALUES (42, 'Sachin', 'MD');
INSERT INTO Customers VALUES (43, 'Kim', 'VA');
INSERT INTO Customers VALUES (44, 'Charlie', 'CA');
INSERT INTO Customers VALUES (45, 'Mark', 'CA');
--SELECT * FROM Customers;

DROP TABLE Customer_Orders;
CREATE TABLE Customer_Orders (Order_ID integer, Customer_ID integer, Order_Date Date);
INSERT INTO Customer_Orders VALUES(36,42,'2018-04-01');
INSERT INTO Customer_Orders VALUES(37,43,'2018-04-07');
INSERT INTO Customer_Orders VALUES(38,45,'2018-05-01');
INSERT INTO Customer_Orders VALUES(39,44,'2018-05-15');
INSERT INTO Customer_Orders VALUES(40,43,'2018-06-02');
INSERT INTO Customer_Orders VALUES(41,41,'2018-07-12');
--SELECT * FROM Customer_Orders;

DROP TABLE Customer_Order_Products;
CREATE TABLE Customer_Order_Products (Order_ID integer, Product_ID integer, Quantity integer);
INSERT INTO Customer_Order_Products VALUES(37,314,3);
INSERT INTO Customer_Order_Products VALUES(38,421,5);
INSERT INTO Customer_Order_Products VALUES(37,510,4);
INSERT INTO Customer_Order_Products VALUES(40,41,39);
INSERT INTO Customer_Order_Products VALUES(41,425,2);
INSERT INTO Customer_Order_Products VALUES(39,314,2);
--SELECT * FROM Customer_Order_Products;

DROP TABLE Products;
CREATE TABLE Products (Product_ID integer, Product_Type VARCHAR(200), Price integer);
INSERT INTO Products VALUES(314, 'Book', 15);
INSERT INTO Products VALUES(421, 'Water Bottle', 10);
INSERT INTO Products VALUES(510, 'Board Game', 25);
INSERT INTO Products VALUES(689, 'Soccer Ball', 20);
INSERT INTO Products VALUES(425, 'Socks', 5);
--SELECT * from Products;

--1. Who placed orders in May?
-- Charlie and Mark
SELECT name FROM Customers WHERE Customer_ID IN (
	SELECT Customer_ID FROM Customer_Orders 
	WHERE MONTH(Order_Date) = 5);
--2. Who did not order any books?
SELECT name FROM Customers WHERE Customer_ID NOT IN (
	SELECT Customer_ID FROM Customer_Orders WHERE Order_ID IN (
		SELECT Order_ID FROM Customer_Order_Products WHERE Product_ID IN (
			SELECT Product_ID FROM Products WHERE Product_Type = 'Book')));
--3. How much (in dollars) of each product has the company sold?
/*
SELECT P.Product_Type, P.Price, COP.Quantity, (P.Price*COP.Quantity) AS Revenue
	FROM Products P
	JOIN Customer_Order_Products COP
	ON P.Product_ID = COP.Product_ID
	ORDER BY Revenue DESC;
*/

SELECT P.Product_Type, SUM ((P.Price*COP.Quantity)) AS REVENUE
	FROM Products P
	JOIN Customer_Order_Products COP
	ON P.Product_ID = COP.Product_ID
	GROUP BY P.Product_Type
	ORDER BY Revenue DESC;

--4. What products were ordered by customers on the east coast?
/*
SELECT Product_Type FROM Products WHERE Product_ID in (
	SELECT Product_ID FROM Customer_Order_Products WHERE Order_ID IN (
		SELECT Order_ID FROM Customer_Orders WHERE Customer_ID IN (
			SELECT Customer_ID FROM Customers WHERE State IN ('VA', 'MD'))));
*/

SELECT P.Product_Type, C.Name, C.State 
	FROM Products P
	JOIN Customer_Order_Products AS COP
		 ON P.Product_ID = COP.Product_ID
	JOIN Customer_Orders AS CO
		 ON COP.Order_ID = CO.Order_ID
	JOIN Customers AS C
		 ON CO.Customer_ID = C.Customer_ID
	WHERE C.State IN ('VA','MD');


--5. List the customers by how many items they ordered. 
/*
SELECT CO.Order_ID, CO.Customer_ID, COP.Product_ID, COP.Quantity
	INTO mytable
	FROM Customer_Orders CO
	JOIN Customer_Order_Products COP
	ON CO.Order_ID = COP.Order_ID;
SELECT C.Name, SUM(M.Quantity) AS Total_Quantity
	FROM Customers C
	JOIN mytable M
	ON C.Customer_ID = M.Customer_ID
	GROUP BY C.Name
	ORDER BY Total_Quantity DESC;
*/

SELECT C.Name, SUM(NEW.Quantity) AS Total_Quantity
	FROM Customers C
	JOIN (SELECT CO.Order_ID, CO.Customer_ID, COP.Product_ID, COP.Quantity
			FROM Customer_Orders CO
			JOIN Customer_Order_Products COP
			ON CO.Order_ID = COP.Order_ID) NEW
	ON C.Customer_ID = NEW.Customer_ID
	GROUP BY C.Name
	ORDER BY Total_Quantity DESC;

