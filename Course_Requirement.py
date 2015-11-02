# -*- coding: utf-8 -*-
__author__ = 'ronfe'

import os
from pymongo import MongoClient
from bson.objectid import ObjectId
from bson.son import SON
import datetime

version = "1.0.0"
fileId = str(ObjectId.from_datetime(datetime.datetime.now()))[:8]

os.chdir(os.path.dirname(os.path.realpath("__file__")))
os.chdir('./reports')
report = open(fileId + '.md', 'w+')

dbClient = MongoClient('mongodb://10.8.3.16:27017')
remotePoints = dbClient['yangcong-prod25']['points']
remoteVideos = dbClient['yangcong-prod25']['videos']

localClient = MongoClient('mongodb://localhost:27017')
localUserRatings = localClient['yangcong']['userRatings']

endDate = datetime.datetime.now() - datetime.timedelta(days=1)
startDate = endDate - datetime.timedelta(days=14)
startId = ObjectId.from_datetime(startDate)
endId = ObjectId.from_datetime(endDate)

# STEP 0 : Print metadata
report.write("# Yangcong Math Video/Topic Data Biweek Report  \n")
report.write("Script Version: " + version + '  \n')
report.write("by @ronfe 2015.  \n\n")
description = "This report is about the description stats of  \n\n* 5 videos with highest average rating (at least 100 ratings) \n* 5 videos with highest average rating (at least 100 ratings) \n* 5 most watched videos, and \n* 5 most finished topics \n\nduring " + str(startDate) + ' and ' + str(endDate) + '.\n'
report.write(description)
# STEP 1 : video Ratings

report.write("## Best/Worst Rated Videos\n")

pipeLine = [
    {"$match": {"_id": {"$gte": startId, "$lt": endId}, "eventKey": "rateVideo"}},
    {"$project": {"videoId": "$video", "userId": "$user", "videoRate": "$eventValue.videoRate"}}
]
tempDocs = list(remotePoints.aggregate(pipeLine))

localUserRatings.insert_many(tempDocs)

pipeLine = [
    {"$group": {"_id": "$videoId", "ratings": {"$push": "$videoRate"}}},
    {"$match": {"ratings.100": {"$exists": True}}},
    {"$unwind": "$ratings"},
    {"$group": {"_id": "$_id", "avgRating": {"$avg": "$ratings"}, "sumN": {"$sum": "$ratings"}}},
    {"$project": {"avgRating": 1, "count": {"$divide": ["$sumN", "$avgRating"]}}},
    {"$sort": SON([("avgRating", -1), ("count", -1)])}
]

videoR = list(localUserRatings.aggregate(pipeLine))

bestV = videoR[0:5]
worstV = videoR[-5:]

report.write('Best rated videos top 5: \n\n')
report.write('| Video Name | Average Rating | Total Ratings |\n')
report.write('|:-----------|:--------------:|:-------------:|\n')
for each in bestV:
    videoName = remoteVideos.find_one({"_id": each['_id']})['name'].encode('utf-8')
    report.write("| " + videoName + ' | ' + str(each['avgRating']) + ' | ' + str(int(each['count'])) + ' |\n')

report.write('\n')
report.write('Worst rated videos top 5: \n\n')
report.write('| Video Name | Average Rating | Total Ratings |\n')
report.write('|:-----------|:--------------:|:-------------:|\n')
for i in range(0, len(worstV)):
    videoName = remoteVideos.find_one({"_id": worstV[len(worstV) - i - 1]['_id']})['name'].encode('utf-8')
    report.write("| " + videoName + ' | ' + str(worstV[len(worstV) - i - 1]['avgRating']) + ' | ' + str(int(worstV[len(worstV) - i - 1]['count'])) + ' |\n')

# Drop temp Collection
localUserRatings.drop()
report.write('\n')


# STEP 2 : video Ratings
report.write("## Most Watched Videos\n")

pipeLine = [
    {"$match": {"_id": {"$gte": startId, "$lt": endId}, "eventKey": "openVideo"}},
    {"$group": {"_id": "$video", "watchers": {"$sum": 1}}},
    {"$sort": SON([("watchers", -1)])}
]

result = list(remotePoints.aggregate(pipeLine))[0:5]

report.write('Most watched videos top 5: \n\n')
report.write('| Video Name | Total Watchings |\n')
report.write('|:-----------|:---------------:|\n')
for each in result:
    videoName = remoteVideos.find_one({"_id": each['_id']})['name'].encode('utf-8')
    report.write("| " + videoName + ' | ' + str(each['watchers']) + ' |\n')


# Step 3 finished topic
report.write('\n')
report.write("## Most Finished Topics Top 5\n")

pipeLine = [
    {"$match": {"isPassed": True}},
    {"$group": {"_id": "$user, $topic", "finishedTopics": {"$addToSet": "$topic"}, "finishedTasks": {"$addToSet": "$task"}}}
]

taskStatus = dbClient['yangcong-prod25']['taskstatuses']
tempResult = list(taskStatus.aggregate(pipeLine))
localClient['yangcong']['finishedTopics'].insert_many(tempResult)

#

report.write('\n')
report.write('You can get this report by accessing ```http://10.8.8.8:2333/reports/' + fileId + '.md```\n\n')
report.write('Report Generated on ' + str(datetime.datetime.now()) + '  \n')
report.write('This script is part of Yangcong MiniData Plan (YMDP)\n')
report.close()