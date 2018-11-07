Spark SQL Workshop


    Task 1: Spark SQL IO
    
        Implement the "readCsv", "readJson", "readOrc" methods in the "prv.saevel.spark.sql.workshop.io.SparkSqlIo"
        object to read Survey class instances from given CSV, JSON lines and ORC files respectively to practice IO
        operations using SQLContext. 
        
        Run the "SparkSqlIoTest" test to verify the correctness of your implementation.

        
    Task 2: Basic Data Frames
    
        Implement the "prv.saevel.spark.sql.workshop.dataframes.EmployeeSalaryLens"'s "apply" method, to take the
        "employees" DataFrame, "assignments" DataFrame and "salaries" DataFrame contaning Employee, Assignment and
        Salary data respectively. It should use those to find names("name" column), surnames("surname" column) and id's
        ("id" column) of employees from the given "department" earning between "minSalary" and "maxSalary".
        
        Run the "EmployeeSalaryLensTest" to verify the correctness of your implementation. 


    Task 3: Aggregations & Functions
    
        Implement the "apply" method in "prv.saevel.spark.sql.workshop.aggregations.SalaryStatistics" so that it takes
        three DataFrames: "employees", "assignments" and "salaries", containing Employee, Assignment and Salary data,
        respectively and for each of the available departments, calculates:
        
            * average salary within the department ("salary_avg")
            * standard deviation (from population) of the salary within the department ("salary_stddev")
            * total count of all employees in the department ("employee_count")
            
        and return all of them in a DataFrame, which, for each record, also contains the "department" field. In the
        implementation, make use of functions available in the "org.apache.spark.sql.functions" package.
        
        Run the "SalaryStatisticsTest" to verify the correctness of your implementation.
       
       
    Task 4: UDFs
        
        Implement the "apply" method in "prv.saevel.spark.sql.workshop.udf.ConsonantsUDF" so that it implements a 
        function that removes all the vowels from a String column in Spark SQL and leaves only consonants (we assume 
        the ASCII consonant / vowel set) and registers this function as an UDF named 'consonants'.
        
        Run the "ConsonantsUDFTest" to verify the correctness of your implementation.
        
        
    Task 5: UDAFs
    
        Implement the missing methods / values in the "prv.saevel.spark.sql.workshop.udaf.HarmonicMeanUDAF" object
        in order to create a Spark SQL UDAF calculating the harmonic mean of the "value" field from multiple rows. 
        
        Harmonic mean is defined (for positive numbers) as described here: https://en.wikipedia.org/wiki/Harmonic_mean
        
        Run the "HarmonicMeanUDAFTest" to verify the correctness of your implementation.
           
        
     
          