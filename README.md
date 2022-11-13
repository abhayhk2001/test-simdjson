# Json to Parquet conversion 

A json file is recieved from IOT devices in equal intervals. 

This file has to be loaded into memory, a pivot operation is applied so we get the value of every sensor on the vehicle which is indexed based on device id and timestamp. After the operation the data should look like:
 
After this operation the table in the memory has to be converted Parquet format.

## Testing Environment
We decided to use VM with limited RAM to replicate final Kubernetes deployment scenario. It uses the Intel IceLake, taking advantage of the latest intel optimizations.

## Changes and Improvements
1.	Pivot table change: The original pivot table did not use Value parameter, using which greatly reduces the number of columns.
2.	Adding SimdJSON: It is a SIMD package used mainly used for JSON parsing.

## Java Implementation, Challenges and Results
After a discussion we implemented the same program in Java. Initially some decisions we made were to use GSON for json parsing and Tablesaw for DataFrame operations. GSON gives a Java Object as a result, then this is converted to Tablesaw Table. Table is then pivoted and converted to Parquet using a supporting library tablesaw-parquet. 

This method did not give any improvement in performance, java is usually used for application development and as a backend programming language. The tablesaw algorithm though it provides decent results Is not optimized for large dataframes and data management. 

I have listed challenges faced below:
1.	An optimized method to handle dataframe (tabular) data.
2.	Tablesaw does not support aggregate functions for strings.
3.	Tablesaw-parquet does not support large strings to be converted to Parquet format.


## Run the Tests

1. Clone the repository.
   ```
   $ git clone https://github.com/abhayhk2001/test-simdjson
   ```

2. Create a virtual environment for python development (virtualenv is used here)
   ```
   $ pip install virtualenv
   $ cd test-simdjson
   $ python -m venv env
   $ source env/bin/activate
   ```

3. Install the python packages necessary and create output directories
   ```
   $ pip install -r requirements.txt
   $ mkdir output extras
   ```

4. Run the main.py file
   ```
   $ python main.py
   ```


The Scripts forlder contains steps to setup VM in Google Cloud Platform VM and Amazon Web Services VM. The run.sh file contains the steps to run the program after setup is complete.