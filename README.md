# data-ingestion

![lint](https://github.com/transitmatters/data-ingestion/workflows/lint/badge.svg?branch=main)
![test](https://github.com/transitmatters/data-ingestion/workflows/test/badge.svg?branch=main)
![deploy](https://github.com/transitmatters/data-ingestion/workflows/deploy/badge.svg?branch=main)

This is an app that can host all our data crunching jobs, especially those that use s3.

It uses Chalice, which makes it easy to add new functions that can be triggered on an interval or via rest API endpoint.

So far we have:

- Store MBTA Alerts data daily.
- Store number of trips with new trains on Orange and Red line daily.
- Store Bluebikes station status data every 5 min.
- Store ridership data
- Process and store speed restrictions

To add a new lambda function, put the methods you need in a new file in chalicelib/.
Then add your trigger in app.py.
Then, add a policy.json file for any permissions you might need in .chalice/, updating .chalice/config.json accordingly.
Lastly, if you need secret environment variables, update resources.json and make sure they are passed
with --parameter-override in the cloudformation deploy step of deploy.sh.
(Non-secret envvars can be added to config.json directly.)
