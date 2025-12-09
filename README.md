Running (from root):
java -cp ".;C:\\Program Files\\CData\\CData JDBC Driver for Salesforce 2025\\lib\\cdata.jdbc.salesforce.jar" SqlPerformanceTester all 5

Notes: needs to specify the class path for your JDBC driver, along with itself. Can specify a speicific query from the queries.sql file, or specify "all." The number at the end is the number of runs for each query.

Rebuilding:
javac SqlPerformanceTester.java
