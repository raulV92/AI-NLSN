# AI-NLSN

Input files information:

In order for the whole validation to run there must be these files in the same folder as the program:

1) GSR.csv : must contain the column 'SMS ID', any column with a two digit number on ots name will be assumed to be an index

2) plan.csv : Must contain the columns 'SMS ID' , 'Activity Local Name' , 'Associate Cdar ID'

3) AI.sqlite

4)CollectedData.csv : Must contain the columns 'SMS ID', 'Entity Id','Fact Id','Value'

--> Result of validation will be displayen on  folder named 'ValidationResult' (Program will create it automatically). 
As a best practice you might delete it before you run the program if folder already exist
