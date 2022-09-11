# CPSC449-WebBackEnd-Wordle-Part3


Introduction

In this project, we developed a RESTful microservice for tracking the state of a game.

·	Starting a new game - A user will be able to start a game only if they have'nt played the game before. For this, the service takes the user id and gameid to check. If the user has not played the game, the user is allowed to start the game and an entry is made in the redis database for that user. If the user id is already present for that input game id, the user is displayed a mention "Cannot start the game".

·	Updating the state of a game - Whenever the user makes a new guess, the number of guesses along with the entered word gets updated in the database for the user id and that game id. To accomplish this, userid, gameid and the guess word needs to be provided. The number of guesses entered cannot be more than 6.

·	Restoring the state of a game - The user can retrieve information about the current state of the game,  including the words guessed so far and the number of guesses remaining. For this, we need the user id and game id from the user in order to retrieve the game state.
For retrieving the statistics (top ten users by wins and streaks), we connected to the three shard databases, the top ten users by wins and streaks were retrieved and inserted into redis database using a sorted set in order to get the user ids in decreasing order of wins and streaks.

Note: We have added a dummy user record to test our services. 

Steps to Run the Processes – 

1. To start the services run the command - foreman start
2. To run cron job , open the crontab file and change the path after “/usr/bin/python3” to where the files are extracted .
Eg. if the tar file is downloaded in Downloads folder as default, and contents are extracted there then the path would look like “/home/student/Downloads/Project4/redisUpdate.py”
Make changes accordingly
3. service cron start
4. To check whether the cron jobs are running - service cron status
5.modifiedMicroService3.py is not included in Procfile (it contains modified code of fetching statistics from redis)(it is just from your reference)
