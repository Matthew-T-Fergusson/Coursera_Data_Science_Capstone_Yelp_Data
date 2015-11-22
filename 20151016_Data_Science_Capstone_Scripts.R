###########################################################
#title: 'Data Science Capstone'
#author: "Matthew Fergusson"
#date: "Oct 16, 2015"
#output: Research Paper 
###########################################################

#Remove all objects in [r] environment
#rm(list = ls(all.names = TRUE))

###########################################
# Set Libraries 
###########################################

#install.packages('jsonlite')
#install.packages('sqldf')
#install.packages('tidyr')
#install.packages('fpc')
install.packages('apcluster')

library(dplyr)
library(plyr)
library(sqldf)
library(jsonlite)
library(caret)
library(randomForest)
library(fpc)
library(apcluster)

###########################################
# Read in data 
###########################################

setwd("C:/Users/mfergusson/Desktop/MTF Personal/Education - Coursera Data Science Specialization/10 - Data Science Capstone/Task 0 - Get the Data/yelp_dataset_challenge_academic_dataset")

business <- stream_in(file("yelp_academic_dataset_business.json"))
#Imported 61184 records / 15 Variables
#checkin <- stream_in(file("yelp_academic_dataset_checkin.json"))
#Imported 45166 records / 3 Variables
review <- stream_in(file("yelp_academic_dataset_review.json"))
#Imported 1569264 records / 8 Variables
tip <- stream_in(file("yelp_academic_dataset_tip.json"))
#Imported 495107 records / 3 Variables
user <- stream_in(file("yelp_academic_dataset_user.json"))
#Imported 366715 records / 11 Variables

#rm(checkin)

business <- flatten(business, recursive = TRUE)
checkin <- flatten(checkin, recursive = TRUE)
review <- flatten(review, recursive = TRUE)
tip <- flatten(tip, recursive = TRUE)
user <- flatten(user, recursive = TRUE)

###########################################
# Quiz questions 
###########################################

#Question 5
  # What percentage of the reviews are five star reviews 
  # (rounded to the nearest percentage point)?

  sum(review$stars == 5) / nrow(review)
  #0.3692986

#Question 7
  # Conditional on having an response for the attribute 
  # "Wi-Fi", how many businesses are reported for having 
  # free wi-fi (rounded to the nearest percentage point)?

  names(business_FLAT)
  unique(business_FLAT$`attributes.Wi-Fi`)
  sum(business_FLAT$`attributes.Wi-Fi` == 'free', na.rm = TRUE) / (sum(business_FLAT$`attributes.Wi-Fi` != "free", na.rm = TRUE) + sum(business_FLAT$`attributes.Wi-Fi` == 'free', na.rm = TRUE))
  #0.4091519

#Question 10
  # What is the name of the user with over 10,000 compliment votes of type "funny"?

  QQ10 <- user_FLAT[user_FLAT$votes.funny > 10000,]
  #68 obs
  QQ10 <- QQ10[QQ10$name %in% c("Brian", "Ira", "Jeff", "Roger"),]
  #1 obs
  
#Question 11
  # Create a 2 by 2 cross tabulation table of when a user
  # has more than 1 fans to if the user has more than 1 
  # compliment of type "funny". Treat missing values as 0
  # (fans or votes of that type). Pass the 2 by 2 table to 
  # fisher.test in R. What is the P-value for the test of independence?

  # TL: Fan >  1 & Funny <= 1
  # TR: Fan >  1 & Funny >  1
  # BL: Fan <= 1 & Funny <= 1
  # BR: Fan <= 1 & Funny >  1

  TL <-  user_FLAT[user_FLAT$fans > 1 ,]
  TL <-  TL[TL$compliments.funny <= 1,]
  TR <-  user_FLAT[user_FLAT$fans > 1 ,]
  TR <-  TR[TR$compliments.funny > 1,]
  BL <-  user_FLAT[user_FLAT$fans <= 1 ,]
  BL <-  BL[BL$compliments.funny <= 1,]
  BR <-  user_FLAT[user_FLAT$fans <= 1 ,]
  BR <-  BR[BR$compliments.funny > 1,]
  
  A = rbind(c(nrow(TL),nrow(TR)),c(nrow(BL),nrow(BR)) )
  
  fisher.test(A)

###########################################
# Profiling data and understanding relationships 
###########################################
  
business_10 <- business[1:10,]
checkin_10 <- checkin[1:10,]
review_10 <- review[1:10,]
tip_10 <- tip[1:10,]
user_10 <- user[1:10,]
rm(tip_FLAT)

write.csv(business[1:10,], file = "business_10.csv")
#View(business[1:10,])
write.csv(checkin[1:10,], file = "checkin_10.csv")
write.csv(review[1:10,], file = "review_10.csv")
write.csv(tip[1:10,], file = "tip_10.csv")
write.csv(user[1:10,], file = "user_10.csv")
#View(user[1:10,])

############################################
#
# Questions for Analysis
#
# 1)
# If reviewers are grouped by their location (based on the central point of their 
# reviews and tips) and the types of businesses they frequent the most (Based on 
# their tips and reviews), how closely do these groupings align with these user's 
# social networks (based on their Yelp friends? 
#
# 2)
# Do users ratings and tips align more with their friend group or other users that 
# frequent similar establishments? 
#
# These questions would be aimed as an exploratory analysis for creating a targeted 
# marketing strategies for local markets.
#
############################################


###########################################
# Find centroid of a users reviews and tips
###########################################

User_Location_Hits <- as.data.frame(rbind(cbind(tip$user_id,tip$business_id),cbind(review$user_id,review$business_id)))
colnames(User_Location_Hits) <- c("user_id", "business_id")
Business_Locations <- as.data.frame(cbind(business$business_id,business$longitude , business$latitude,business$city, business$state,business$full_address))
colnames(Business_Locations) <- c("business_id", "longitude", "latitude","city", "state","full_address")

#Get the location of all reviews and tips from a user
User_Locations <- sqldf('
      Select 
       A.user_id
      ,A.business_id
      ,B.longitude
      ,B.latitude
      FROM User_Location_Hits A
      LEFT JOIN Business_Locations B
      ON A.business_id = B.business_id
      ')

#get the centroid of the x,y lattitude / longitude coordinates of reviews and tips
User_Locations_Centroid <- sqldf(' 
      SELECT
       user_id
      ,sum(longitude)/count(longitude) as longitude_Centroid 
      ,sum(latitude)/count(latitude) as latitude_Centroid
      FROM User_Locations
      group by user_id 
      ')

#Calculate the distance from the centroid of all reviews and tips
User_Locations <- sqldf('
    Select 
     A.user_id
    ,A.business_id
    ,A.longitude
    ,A.latitude
    ,B.longitude_Centroid
    ,B.latitude_Centroid
    ,cAST(POWER( POWER(A.longitude - B.longitude_Centroid,2.00) + POWER(A.latitude - B.latitude_Centroid,2.00),.5) as decimal(10,8)) as Distance_From_Centroid
    FROM User_Locations A
    LEFT JOIN User_Locations_Centroid B
    ON A.user_id = B.user_id
    ')  

#Calculate average distance from the centroid of all reviews for each user
User_Locations_Summary <- sqldf('
    Select 
     A.user_id
    ,A.longitude_Centroid
    ,A.latitude_Centroid
    ,Count(A.Distance_From_Centroid) as Tip_or_Review_Count
    ,AVG(A.Distance_From_Centroid) as AVG_Distance_From_Centroid
    ,STDEV(A.Distance_From_Centroid) as STDEV_Distance_From_Centroid
    FROM User_Locations A
    Group by A.user_id 
    ,A.longitude_Centroid
    ,A.latitude_Centroid
    ')  

###########################################
# Find location to test for locals
###########################################

#distribution of tips and reviews by city and state
Business_Location_Dist <- sqldf('
SELECT 
B.city,B.state
,count(B.state) as Counts
FROM User_Location_Hits A
left join Business_Locations B
ON A.business_id = B.business_id
group by city,state
')

#distribution of Businesses by city and state
Business_Location_Dist2 <- sqldf('
SELECT 
B.city,B.state
,count(B.state) as Counts
FROM Business_Locations B
group by city,state
')

###########################################
# Find Users and businesses wihtin defined region
###########################################

#Get only reviewers located central to ASU by 25 miles radius 
  #This distance covers a large portion of Phoenix, Scottsdale, Tempe, Mesa, Gilbert and Glendale which are all well represented in this data set
  #33.446492, -111.928276 lattitude / longitude ASU
  #1 degree of lattitude = ~69 miles

  #25/69
  #0.3623188 degrees a reveiwer can be away from Washington DC

User_Locations_Summary_ASU <- sqldf('
    Select 
     user_id
    ,longitude_Centroid
    ,latitude_Centroid
    ,Tip_or_Review_Count
    ,AVG_Distance_From_Centroid
    ,STDEV_Distance_From_Centroid
    FROM User_Locations_Summary A
    where cAST(POWER( POWER(-111.928276 - longitude_Centroid,2.00) + POWER(33.446492 - latitude_Centroid,2.00),.5) as decimal(10,8)) < 0.3623188
    ')  
    #107980 users in this area

Business_Locations_ASU  <- sqldf('
  SELECT 
  business_id
  ,longitude
  ,latitude
  FROM Business_Locations A
  WHERE cAST(POWER( POWER(-111.928276 - longitude,2.00) + POWER(33.446492 - latitude,2.00),.5) as decimal(10,8)) < 0.3623188
  ')  
  #22457


###########################################
# Define how to Identify Local Users
###########################################

#define local user as someone who lives within easy driving 
#distance at the speed of most local roads of places they have reviewed
  #radius allowed by drive at 45mph for 15min
  #45*(15/60)
  #11.25 mi = average distance of all reviews from an individuals centroid
  #11.25/69 
  #0.1630435 deg = long/lat distance
  #if avg distance from centroid + 3 * standard deviation of distance <= 0.1086957
  #then approximately 99.9% of that reviewers reviews are within that radius specified

Local_User_Locations_Summary_ASU <- sqldf('
SELECT
user_id
,longitude_Centroid
,latitude_Centroid
,Tip_or_Review_Count
,AVG_Distance_From_Centroid
,STDEV_Distance_From_Centroid
FROM User_Locations_Summary_ASU
WHERE AVG_Distance_From_Centroid + 3 * STDEV_Distance_From_Centroid <= 0.1630435
')
#78652 users fitting local description

#limit reviewers to those that have 3 or more reviews of unique and under 250
  #purpose to get reviewers with at least a few reviews and avoid spammers

Unique_Local_Review_Tips <- sqldf('
SELECT 
A.user_id
,A.business_id
FROM User_Locations A
WHERE A.user_id in (SELECT B.user_id FROM Local_User_Locations_Summary_ASU B group by B.user_id)
GROUP BY A.user_id
,A.business_id
')
#150777

Unique_Local_Users <- sqldf('
SELECT 
A.user_id
,Count(A.user_id) as Counts
FROM Unique_Local_Review_Tips A
GROUP BY A.user_id
HAVING Count(A.user_id) >=3
')
#12762

Local_User_Locations_Summary_ASU_FINAL <- sqldf('
SELECT A.*
FROM Local_User_Locations_Summary_ASU A
WHERE A.user_id in (SELECT B.user_id FROM Unique_Local_Users B group by B.user_id)
')
#12762

#Can remove tip table now because no longer needed to save memory
#rm(tip)

  
###########################################
# Identify clusters of local Users based on location
###########################################

#View Plot
  plot(Local_User_Locations_Summary_ASU_FINAL$longitude_Centroid,Local_User_Locations_Summary_ASU_FINAL$latitude_Centroid, col = "blue" )

#create lat lon table
  User_Lat_Lon <- as.data.frame(cbind(Local_User_Locations_Summary_ASU_FINAL$longitude_Centroid,Local_User_Locations_Summary_ASU_FINAL$latitude_Centroid))

#Determine # of clusters
  # Determine number of clusters (squared error (SSE))
  wss <- (nrow(User_Lat_Lon)-1)*sum(apply(User_Lat_Lon,2,var))
  for (i in 2:50) wss[i] <- sum(kmeans(User_Lat_Lon, 
                                       centers=i)$withinss)
  plot(1:50, wss, type="b", xlab="Number of Clusters",
       ylab="Within groups sum of squares")
  #looks like 5-7 looks to be the end of the curve
  
  library(fpc)
  # Determine number of clusters (PAMK)
    #runs too long
    pamk.best <- pamk(User_Lat_Lon)
    cat("number of clusters estimated by optimum average silhouette width:", pamk.best$nc, "\n")
    plot(pam(User_Lat_Lon, pamk.best$nc))

#K nearest means with 10
  set.seed(1986)
  kmeansOBJ <- kmeans(User_Lat_Lon, centers = 6, iter.max = 1000)
  plot(User_Lat_Lon, col = kmeansOBJ$cluster)

#table for users in similar areas geographically
  
A = as.data.frame(Local_User_Locations_Summary_ASU_FINAL$user_id)
Cluster_Local_User <- (cbind(A, 
      Local_User_Locations_Summary_ASU_FINAL$longitude_Centroid,
      Local_User_Locations_Summary_ASU_FINAL$latitude_Centroid,
      kmeansOBJ$cluster))
colnames(Cluster_Local_User) <- c("user_id","longitude_Centroid","latitude_Centroid", "cluster")


###########################################
# Identify Reviewed Businesses reviewed by local Users
# grouped by geographical User clusters
###########################################

#all local user reviews in population
Local_Business_Reviews <- sqldf('
SELECT * 
FROM review A 
WHERE A.user_id in (SELECT B.user_id FROM Local_User_Locations_Summary_ASU_FINAL B group by B.user_id)
')


#all local businesses reviewed by local reviewers
Local_Businesses_Reviewed_By_Locals <- sqldf('
SELECT A.*
, B.cluster
FROM Local_Business_Reviews A 
LEFT JOIN Cluster_Local_User B
ON A.user_id = B.user_id
WHERE A.business_id in (SELECT C.business_id FROM Business_Locations_ASU C group by C.business_id)
')

###########################################
# Identify Businesses Liked by locals
# Avg rating in 95 percentile = 4.738558
# must have >= 10 ratings
# not including clusters for sake of time
###########################################

Local_Businesses_Reviewed_By_Locals_avg_ratings <-sqldf('
SELECT A.business_id
,AVG(A.stars) as AVG_stars
,Count(A.stars) as Counts
FROM Local_Businesses_Reviewed_By_Locals A 
Group by A.business_id
having Count(A.stars) >= 10
')
#1486

mean(Local_Businesses_Reviewed_By_Locals_avg_ratings$AVG_stars)
#3.87798
quantile(Local_Businesses_Reviewed_By_Locals_avg_ratings$AVG_stars, c(.8,.85,.90,.95 ,.99))
#     80%      85%      90%      95%      99% 
#4.363636 4.461538 4.594872 4.738558 5.000000  

Local_Businesses_Reviewed_By_Locals_avg_ratings <-sqldf('
SELECT A.*
, case when A.AVG_stars >= 4.875000 then "Y" else "N" end as Highly_Liked
FROM Local_Businesses_Reviewed_By_Locals_avg_ratings A 
')

###########################################
# Create model to identify the types of 
# businesses locals like 
###########################################

#all businesses in the data set
business_Analysis <- business[business_Analysis$business_id %in% Local_Businesses_Reviewed_By_Locals_avg_ratings$business_id, ] 
#using hours and attributes city
business_Analysis_1 <- cbind(business_Analysis[1],business_Analysis[5:6],business_Analysis[5:6],business_Analysis[14:53],business_Analysis[55:105])

Local_Businesses_Reviewed_By_Locals_avg_ratings$Highly_Liked

#create data set to test what locals really like
a <- as.data.frame(unique(unlist(business_Analysis$categories)))

###########################################
# Create lookup tables for lists fields 
###########################################
rm(user_friends_0)
rm(user_friends_3)
rm(user_friends_4)
rm(a)

#create object for loop
user_friends_4 <- data.frame()

#[y,x]
#[row,column]

#user count for loop
Business_Count <- nrow(business_Analysis)
#Create data set with just user names and friends
Business_Cat_0 <- data.frame(cbind(business_Analysis$business_id, business_Analysis$categories))
#Chang users with no friends to NAs
Business_Cat_0$X2[Business_Cat_0$X2 %in% c('character(0)')] <- NA
#Convert lists in dataframe to strings
Business_Cat_0$X3 <- sapply(Business_Cat_0$X2, toString)


#for (i in 1:User_Count) {
for (i in 1:Business_Count) {
  #get user name
  UserName = Business_Cat_0[i,1]
  #get the users friends in a list
  user_friends_1 <- as.character(Business_Cat_0[i,3])
  #split the users friends into a separated list that can be looped through
  user_friends_1 <- unlist(strsplit(user_friends_1, split=","))
  #get count of that users friends in that list
  Friend_Count <-length(user_friends_1)
  
  #Loop through all friends and create data frame
  for (j in 1:Friend_Count) {
    #get friend names
    user_friends_3 <- cbind(UserName,user_friends_1[j] )
    #concatentate friend names into a table with all Users and their friends
    user_friends_4 <- rbind(user_friends_4, user_friends_3) 
  }  
}



a <- as.data.frame(unlist(business$categories[1]))
