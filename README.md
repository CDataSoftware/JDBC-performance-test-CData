##Rebuilding the Java Class  
```javac SqlPerformanceTester.java```  

Queries exist in queries.sql. The name for a query is padded by $$. Query is on the next line.  

**Note:** specify the class path for your JDBC driver, along with root directory itself.  
Can specify a speicific query from the queries.sql file, or specify "all."   
The number at the end is the number of runs for each query.  

##Running (from root):  
```java -cp ".;C:\\Program Files\\CData\\CData JDBC Driver for Salesforce 2025\\lib\\cdata.jdbc.salesforce.jar" SqlPerformanceTester [queryName|all] #```  
#Command to run - Query named Q1, run 5 times   
```java -cp ".;C:\Program Files\CData\CData JDBC Driver for Salesforce 2025\lib\cdata.jdbc.salesforce.jar" SqlPerformanceTester Q1 5```  
#Command to run - All queries, run 5 times  
```java -cp ".;C:\Program Files\CData\CData JDBC Driver for Salesforce 2025\lib\cdata.jdbc.salesforce.jar" SqlPerformanceTester all 5```  

#Running this performance testing harness requires licensed access to a CData driver.  
#Intended for demonstration purposes only.  
#No liability or warantee provided. Use at your own discretion.  
