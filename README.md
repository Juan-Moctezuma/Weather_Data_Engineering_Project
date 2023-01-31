# Weather Data Engineering Project

### Technical Description in Non-Technical Terms
The purpose of this data project is to demonstrate how weather data from multiple cities in the U.S. gets extracted or fetched from *OpenWeather*, an open source (specifically an API - Application Programming Interface). Then it gets transformed or converted into the correct format, and finally loaded into a Postgre SQL database and into a Google Sheet (required step for data visualization with Tableau). The "Extraction Transforming & Loading" (ETL) process mentioned previously was completed with Python programming language across multiple scripts or files. However, additional processes include the automation of workflow (with Apache Airflow), and the use of several external technologies such as Amazon Web Services (AWS), Azure Data Studio, Docker & Google Developement software (credentials). 

### Diagram For Technologies involved
<img src="Assets/Diagram.png" width="50%">

### Summarized Explanation: 
1. Raw data flows from the OpenWeather API (free-source) - Account is required to request an API 'Key'
2. Data modified through the Python's code ETL pipeline - which is automated by Apache Airflow and it's dependent on Docker
   - ETL gets automated as it runs in a given timeframe
   - As part of the automation project, a 'Delete' script gets ran to erase old weather data
3. Data ends up in two places:
   - Info. gets stored in a database - which is set up by Amazon Web Services (AWS) and uses Azure Data Studio (Graphical User Interface to run queries)
   - Info. gets iterated into a Google Sheet (Google Developers' credentials required), which gets connected with Tableau Public (Data Visualization)
  
<img src="Assets/Tableau_results.gif" width="80%">
