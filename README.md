# Campaign Analysis Reporting

## Project Installation and Configuration
### Install dependencies
You may need to install package dependencies
```
pip install -r requirements.txt
```

### Configuration
You should have a local configuration file in project root folder. I've added a sample env file and you can reference it to have your own local env file according to your server configurations.
```
cp env.json.example env.json
nano env.json
```
You might not be sure about some env vars in the configuration file `env.json`. Let me give you a summary.
```json
{
    "MONGODB_HOST": "localhost", // MongoDB Host
    "MONGODB_PORT": 27017,       // MongoDB Port
    "DB_NAME": "admin",          // Database name
    "EVENTS_TABLE_NAME": "events",
    "CONTACTS_REPORT_RESULTS_TABLE_NAME": "reports",
    "REPORT_ANALYSIS_TIME_WINDOW": 10,
    "SAMPLE_DATA_FILE_NAME": "k.csv"
}
```
Here, `EVENTS_TABLE_NAME` is name of collection that has event logs, and `CONTACTS_REPORT_RESULTS_TABLE_NAME` is name of collection that will have the report result. This collection will be used to query some criterias.

`REPORT_ANALYSIS_TIME_WINDOW` is the window size of time delta, which will be used to get contact events that caused specific users to deposit or login to the system. If you leave it, it will be 10 hours by default.

Sometimes you may need to execute this script with test/sample data. In that case, please follow the instructions bellow.
- excute `export REPORT_PROJECT_ENV=dev` in UNIX or `set REPORT_PROJECT_ENV=dev` in windows OS.
- place the sample file you exported from database in the prent folder of this project. At this time, please be sure that you gave the correct DB configuration and collection names because this script might update the production database in `dev` mode.
- Open your local env file `env.json` and give the correct file name to `SAMPLE_DATA_FILE_NAME` key.
Now you are ready to give it a try.

## Running the script
If you finished the above things, then you should be able to execute the script to get the report. The following script will create the reports collection. In case of `dev` environment, this script will parse the sample file to create the collection specified by `EVENTS_TABLE_NAME`.

### Getting contacts report
```shell
python run_contacts.py --all
python run_contacts.py --date 2018-12-31
python run_contacts.py extracoins-december18 sms 2018-12-31
```
Unless you gave the date, then the script will consider that you would want to get report for yesterday.

### Getting freebies report
This is the same as the above but script file name is different.
```shell
python run_freebies.py --all
python run_freebies.py --date 2018-12-31
python run_freebies.py extracoins-december18 sms 2018-12-31
```

### Getting all reports
In this case there are only 2 cases 
- one is to get report for all data 
```shell
python run.py --all
```
- another is to get report for specific date
```shell
python run.py --date 2018-12-31
python run.py --date --today
python run.py --date --yesterday
python run.py --date
```

## Remarks
### Issues or Inconvenience of DB Structure
With the current structure of event logging, it wasn't able to figure out the way to create reports. The pain in the neck was to find purpose event (`login` or `deposit`) linking to specific contact event. So I decided to add two new key in the purpose events so that we can execute the `lookup` query. One is `REPORT_CONTACT_REF_FIELD_NAME`, which store the contact event id. Also another is `REPORT_CONTACT_TIME_DELTA_FIELD_NAME`, which store the time delta between contact event's time stamp and the purpose event's time stamp. So later this field was used to identify whether the purpose event was led by the latest contact event or not.
### Suggestion
I suggest to update the current backend workflow so every purpose event (`login` or `deposit`) has the latest contact event happened before the purpose event.