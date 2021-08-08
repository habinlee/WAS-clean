# WAS-clean
WAS Organization Tool
- Background
- Approach Strategy
- Purpose
- Detailed Function Design
  - Main Function
  - Target File Check
  - Target File Upload
  - Data Loss prevention
  - Additional Functions
- Implementation
  - Log File
  - Target Project Selection Process
  - File Upload process


Background
This is a tool for conveying files and storing them in the NCP Storage to prevent storage overcapacity as the file storage space usage increases.
Current WAS storage usage : 
![image5](https://user-images.githubusercontent.com/88265967/128624161-a9f5ca88-0b97-4d24-ab23-d62b556c71c7.png)
The NCP Archive Storage has been selected as the storage


Approach Strategy
![image6](https://user-images.githubusercontent.com/88265967/128624167-06101596-aaea-44a4-a3f0-eab84b53a694.png)

- From the information above, I confirmed that the result/upload directories should be the first to be organized since they take up most of the capacity
- The Upload directory is classified by project and the Result directory is classified by task
- Basically the point of this tool is to get rid of outdated files, so I set the target files based on how long it has been since the project was closed/done -> The specific time period is going to be given by the user directly
- To make it easy for anyone to look into the transferred data, I am going to upload the files into the new storage with the exact same file structure as its original one
- Process steps : Retrieve a list of projects that are done in the DB -> From that list, filter the projects that were done n months ago, where n is the user input value -> Upload -> If upload was successful, delete from the original storage


Purpose
- Securing server storage capacity by moving unneeded files in terms of system maintenance / management
- To be able to use the data later on when necessary, store it in NCP Archive storage which is a long term storage -> Maintain the data stored available for access / use anytime


Detailed Function Design
Main Function
- User input : month -> select files from n months ago based on the time now for transferring
- DB access info is going to be stored as environment variables
- The log file is going to be written in two formats -> Normal log file / Checkpoint recording file
- Use swift client API to communicate dynamically with Archive Storage (Python 3)

Target File Check
- INPUT : Month value
- OUTPUT : 
  - Returns an ascending list of projects that match the complete date criteria
  - Upload / Result each have their own processes that are sequentially executed and shown
  - Projects that are in the DB but do not exist in the WAS storage are excluded from the list

Target File Upload
- If there are results from the check process
  - Upload? [Y/N/D] :
    - Y -> Target files are uploaded
    - N -> Process termination
    - D -> dry run of the whole process
- INPUT : List of projects returned from the checking process 
- OUTPUT :
    - Files are uploaded into the new storage in the list order
    - Successfully uploaded files are deleted from the WAS storage
- Dry run -> When selected, the entire process is shown on the console without its actual execution
- When transferring data, the most important thing we need to consider is preventing data loss. In this service two main features were applied to keep the data safe
  - Multiplicated checkpoint files for keeping data conveying processes recorded and their states kept track of 
  - Checking the hash value / file size before and after uploading the file and comparing the hash / size of the original file and the transferred file before deleting that file
    - To validate that the data that was transferred is exactly the same as the original one and we are not losing any data

Data Loss Prevention
- When transferring data, the most important thing we need to consider is preventing data loss. In this service two main features were applied to keep the data safe
  - Multiplicated checkpoint files for keeping data conveying processes recorded and their states kept track of 
  - Checking the hash value / file size before and after uploading the file and comparing the hash / size of the original file and the transferred file before deleting that file
    - To validate that the data that was transferred is exactly the same as the original one and we are not losing any data

Additional Functions
- Batch delete function that deletes selected files in the WAS as a whole -> Prepared for possible special cases but not sure if it is going to be used ever. The upload process has a delete function of its own
- Checking the list of files in the new storage -> for confirming the successful upload of files


Implementation
Log File
![image3](https://user-images.githubusercontent.com/88265967/128624221-548c489a-7b2c-4100-8680-48ca3a4a34c7.png)

- There are two forms of log files -> Normal log file for basic logging purposes (log_info_nas_cleaner) / Checkpoint file prepared for error situations (loag_access_nas_cleaner)
- Normal log file :
  - Process start / end
  - Logging of each step of the process -> Target selection / upload / delete success/error
  - Timestamp of each process -> South Korea local time
  - Backup file created every midnight
- Checkpoint File
![image1](https://user-images.githubusercontent.com/88265967/128624243-471d4540-410e-4d22-a1aa-a01e11150dc5.png)

  - Operate a duplicated system of checkpoint files -> log_access_nas_cleaner_1, log_access_nas_cleaner_2
    - To prepare for error situations where the checkpoint info is not successfully recorded, there is always a backup file that can reduce the resource loss to the minimum
      - Got the idea from the server rolling deployment process
    - In the first execution both files are recorded -> From then on comparing the last record time of the two files, data is retrieved from the recent file and written into the relatively older file
    - The checkpoints are recorded on two files alternately and the files backup each other so there is no need to go through the same data again when errors occur
    - Planning on increasing the number of files for better functionality
  - How to confirm error situation / first execution from checkpoint files :
    - The information recorded in checkpoint files :
![image4](https://user-images.githubusercontent.com/88265967/128624264-eccb6fb4-13e9-4b24-a3f8-f73cc0ef946f.png)

      - The checkpoint file contains :
        - Start date of the data selection criteria
        - End date of the data selection criteria
        - project ID directories that have been successfully uploaded and deleted
          - works as a savepoint for when the process had an error and has to be rerun
        - ‘End Successful’ flag
      - First execution is when both of the checkpoint files do not exist -> is_first = True
      - Error case is detected when the ‘End successful’ flag is not found at the end of the file -> emergency = True
      
    - At normal cases :
    - The new process starts from the previous end date
      - ex) Looking at the first ~ 2nd executions, the first was successful
        - The first execution transferred data up to ‘3 2020-03-18’ which means data from at most 3 months before (2019-12-18) are selected
        - The 2nd execution was 2020-06-18 and the month input was 3 -> Data in projects that were done up until 2020-03-18 are selected as targets
        - The new data date range for 2nd execution is : 2019-12-18 ~ 2020-03-18
    - At error cases :
      - The new process starts from the previous start date 
      - ex) Looking at the 2nd ~ 3rd executions, the 2nd execution had an error.
        - The 2nd execution was unsuccessful and the process was not completely finished. We don’t know if all the target data was uploaded/removed or not
        - The 3rd execution date is 2020-09-18 with input 2 then the data date range is : 2019-12-18 ~ 2020-07-18
          - The start date is retrieved from the previous unsuccessful process
        - But the 3rd process does not go through every data in the target range again. From the project ID list from the last checkpoint file, it checks the last pid that has been successfully uploaded and starts from the next project and on.
          - Projects are incremental so the order does not change
  - The purpose of recording the project ID along the process
    - From the error case above, the 3rd process includes the data in the date range of all of the 2nd and 3rd executions 
      - very inefficient at this point
      - in the 3rd execution we have to go through all the data that we probably have been looking at before again
    - But the 3rd process does not go through every data in the target range again. From the project ID list from the last checkpoint file, it checks the last pid that has been successfully uploaded and starts from the next project and on.
      - Projects are incremental so the order does not change
    - Reduces resource losses and increases efficiency

Target Project Selection Process
- Common Information :
  - Projects are retrieved from the DB using SQL queries
  - Projects that do not exist in the server are excluded
  - year / month / day info are all compared and data from n months before based on the date now is retrieved
- Upload Directory :
  - Classified by project ID
  - First execution (is_first)
    - Error case (Emergency)
      - First execution + emergency means that all previous processes failed and this is another execution that targets data starting from the very bottom of the DB
      - Retrieves information from the previous error containing checkpoint  and writes a new checkpoint file
    - No error (not Emergency)
      - This is the actual first execution -> both checkpoint files are written
  - Not first execution (not is_first)
    - Error case (Emergency)
      - Previous run had an error and didn’t finish completely, so the new execution targets data starting from the previous data start date criteria
      - Compare the last write time of the two checkpoints and update the older one
- Result Directory :
  - Classified by task
  - First execution (is_first)
    - Error case (Emergency)
      - First execution + emergency means that all previous processes failed and this is another execution that targets data starting from the very bottom of the DB
      - Retrieves information from the previous error containing checkpoint  and writes a new checkpoint file
    - No error (not Emergency)
      - This is the actual first execution -> both checkpoint files are written
  - Not first execution (not is_first)
    - Error case (Emergency)
      - Previous run had an error and didn’t finish completely, so the new execution targets data starting from the previous data start date criteria
      - Compare the last write time of the two checkpoints and update the older one

File Upload Process
![image2](https://user-images.githubusercontent.com/88265967/128624373-901400ab-1e25-4523-873c-87febaebaf11.png)

- Every upload process (directory/folder creation, file upload) must have a upload/generation checking process to prevent error cases
![image7](https://user-images.githubusercontent.com/88265967/128624389-2664adf8-ae65-42c4-823b-13b3d71af21e.png)

- The uploaded files in the new storage will have the exact same file system structure as the WAS server
- File upload process
  - Upload files in the WAS server sequentially by project ID
  - Use get_object method to check if the file is already in the storage
    - If it exists in the storage, compare the hash / size of the original file in the WAS and the file in the storage to see if they are the same files
    - If they are the same, consider it as an error in the deleting process and simply delete the file in the WAS server storage
  - If the file does not exist in the storage upload process proceeded
    - Put_object method to upload files -> check file upload success by the retrieving the file that has been just uploaded using the get_object method
    - Compare the hash/size of the two files 
    - Upload -> Call the uploaded file -> compare hash / size
  - If the upload process ends successfully, delete from the WAS storage
    - If there is an error in the check / upload process the next rerun execution of the process will follow up with the previous process
    - The uploaded files are deleted in units of project directories
