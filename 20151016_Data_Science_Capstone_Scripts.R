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
#install.packages('apcluster')
#install.packages('maps')

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

quantile(Business_Location_Dist$Counts, c(.8,.85,.90,.95,.96,.97 ,.99))
#     80%      85%      90%      95%      96%      97%      99% 
#  290.80   472.60  1506.20  8417.00 15475.80 32889.84 89163.16 

Business_Location_Dist_Hist <- Business_Location_Dist[Business_Location_Dist$Counts > 25000.00,]
Business_Location_Dist_Hist <- Business_Location_Dist_Hist[order(Business_Location_Dist_Hist$Counts),]

opt <- options("scipen" = 20)
barplot(Business_Location_Dist_Hist$Counts,names=Business_Location_Dist_Hist$city,ps = 2, cex.axis = .7,cex.names = .7,  xpd=TRUE)


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

#install.packages("ggmap")
  library(ggplot2)
  library(ggmap)  

#Google API map plot
  # getting the map
  map_users <- get_map(location = c(lon = mean(Local_User_Locations_Summary_ASU_FINAL$longitude_Centroid), lat = mean(Local_User_Locations_Summary_ASU_FINAL$latitude_Centroid)), zoom = 10,
                          maptype = "hybrid", scale = 2)
  # plotting the map with some points on it 
  ggmap(map_users) +
    geom_point(data = User_Lat_Lon, aes(x = Local_User_Locations_Summary_ASU_FINAL$longitude_Centroid, y = Local_User_Locations_Summary_ASU_FINAL$latitude_Centroid ,  fill = "blue", alpha = 0.2), size = 1, shape = 22) +
    guides(fill=FALSE, alpha=FALSE, size=FALSE) + 
    geom_point()
    
  

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
    #pamk.best <- pamk(User_Lat_Lon)
    #cat("number of clusters estimated by optimum average silhouette width:", pamk.best$nc, "\n")
    #plot(pam(User_Lat_Lon, pamk.best$nc))

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
# Avg rating in 80 percentile = 4.363636
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
, case when A.AVG_stars >= 4.363636 then "Y" else "N" end as Highly_Liked
FROM Local_Businesses_Reviewed_By_Locals_avg_ratings A 
')

###########################################
# Create model to identify the types of 
# businesses locals like 
###########################################

#all businesses in the data set
business_Analysis <- business[business$business_id %in% Local_Businesses_Reviewed_By_Locals_avg_ratings$business_id, ] 
#using hours and attributes city
business_Analysis_1 <- cbind(business_Analysis[1],business_Analysis[14:53],business_Analysis[55:105])

###########################################
# Create lookup tables for categories
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

user_friends_4 <- unique(user_friends_4)
user_friends_5 <- as.data.frame(user_friends_4)
colnames(user_friends_5) <- c("business_id", "categories")
user_friends_5$business_id <- as.character(user_friends_5$business_id)
user_friends_5$categories <- as.character(user_friends_5$categories)

###########################################
# limit to restaurants
###########################################

Restaurants_Analysis  <-sqldf('
SELECT A.business_id
,A.categories
FROM user_friends_5 A 
WHERE A.categories like "%Restaurant%"
') 

#All other categories associated with restaurants
Restaurants_Analysis_2  <-sqldf('
SELECT A.business_id
,A.categories
FROM user_friends_5 A
JOIN Restaurants_Analysis B
ON A.business_id = B.business_id
WHERE A.categories not like "%Restaurant%"
')

AA <- as.data.frame(unique(Restaurants_Analysis_2$categories))

write.csv(AA, file = "Unique_Restaurant_Categories.csv")

#Flags for categories
Restaurants_Analysis_3  <-sqldf(' 
SELECT A.business_id
,MAX(CASE WHEN A.categories like "%Active Life%" THEN 1 ELSE 0 END) AS [category_Active Life]
,MAX(CASE WHEN A.categories like "%Afghan%" THEN 1 ELSE 0 END) AS [category_Afghan]
                                ,MAX(CASE WHEN A.categories like "%American (New)%" THEN 1 ELSE 0 END) AS [category_American (New)]
                                ,MAX(CASE WHEN A.categories like "%American (Traditional)%" THEN 1 ELSE 0 END) AS [category_American (Traditional)]
                                ,MAX(CASE WHEN A.categories like "%Arcades%" THEN 1 ELSE 0 END) AS [category_Arcades]
                                ,MAX(CASE WHEN A.categories like "%Arts & Entertainment%" THEN 1 ELSE 0 END) AS [category_Arts & Entertainment]
                                ,MAX(CASE WHEN A.categories like "%Asian Fusion%" THEN 1 ELSE 0 END) AS [category_Asian Fusion]
                                ,MAX(CASE WHEN A.categories like "%Automotive%" THEN 1 ELSE 0 END) AS [category_Automotive]
                                ,MAX(CASE WHEN A.categories like "%Bagels%" THEN 1 ELSE 0 END) AS [category_Bagels]
                                ,MAX(CASE WHEN A.categories like "%Bakeries%" THEN 1 ELSE 0 END) AS [category_Bakeries]
                                ,MAX(CASE WHEN A.categories like "%Barbeque%" THEN 1 ELSE 0 END) AS [category_Barbeque]
                                ,MAX(CASE WHEN A.categories like "%Bars%" THEN 1 ELSE 0 END) AS [category_Bars]
                                ,MAX(CASE WHEN A.categories like "%Basque%" THEN 1 ELSE 0 END) AS [category_Basque]
                                ,MAX(CASE WHEN A.categories like "%Beer%" THEN 1 ELSE 0 END) AS [category_Beer]
                                ,MAX(CASE WHEN A.categories like "%Brazilian%" THEN 1 ELSE 0 END) AS [category_Brazilian]
                                ,MAX(CASE WHEN A.categories like "%Breakfast & Brunch%" THEN 1 ELSE 0 END) AS [category_Breakfast & Brunch]
                                ,MAX(CASE WHEN A.categories like "%Breweries%" THEN 1 ELSE 0 END) AS [category_Breweries]
                                ,MAX(CASE WHEN A.categories like "%British%" THEN 1 ELSE 0 END) AS [category_British]
                                ,MAX(CASE WHEN A.categories like "%Bubble Tea%" THEN 1 ELSE 0 END) AS [category_Bubble Tea]
                                ,MAX(CASE WHEN A.categories like "%Buffets%" THEN 1 ELSE 0 END) AS [category_Buffets]
                                ,MAX(CASE WHEN A.categories like "%Burgers%" THEN 1 ELSE 0 END) AS [category_Burgers]
                                ,MAX(CASE WHEN A.categories like "%Butcher%" THEN 1 ELSE 0 END) AS [category_Butcher]
                                ,MAX(CASE WHEN A.categories like "%Cafes%" THEN 1 ELSE 0 END) AS [category_Cafes]
                                ,MAX(CASE WHEN A.categories like "%Cajun/Creole%" THEN 1 ELSE 0 END) AS [category_Cajun/Creole]
                                ,MAX(CASE WHEN A.categories like "%Cambodian%" THEN 1 ELSE 0 END) AS [category_Cambodian]
                                ,MAX(CASE WHEN A.categories like "%Cantonese%" THEN 1 ELSE 0 END) AS [category_Cantonese]
                                ,MAX(CASE WHEN A.categories like "%Car Wash%" THEN 1 ELSE 0 END) AS [category_Car Wash]
                                ,MAX(CASE WHEN A.categories like "%Caribbean%" THEN 1 ELSE 0 END) AS [category_Caribbean]
                                ,MAX(CASE WHEN A.categories like "%Caterers%" THEN 1 ELSE 0 END) AS [category_Caterers]
                                ,MAX(CASE WHEN A.categories like "%Cheesesteaks%" THEN 1 ELSE 0 END) AS [category_Cheesesteaks]
                                ,MAX(CASE WHEN A.categories like "%Chicken Wings%" THEN 1 ELSE 0 END) AS [category_Chicken Wings]
                                ,MAX(CASE WHEN A.categories like "%Chinese%" THEN 1 ELSE 0 END) AS [category_Chinese]
                                ,MAX(CASE WHEN A.categories like "%Cocktail Bars%" THEN 1 ELSE 0 END) AS [category_Cocktail Bars]
                                ,MAX(CASE WHEN A.categories like "%Coffee & Tea%" THEN 1 ELSE 0 END) AS [category_Coffee & Tea]
                                ,MAX(CASE WHEN A.categories like "%Comfort Food%" THEN 1 ELSE 0 END) AS [category_Comfort Food]
                                ,MAX(CASE WHEN A.categories like "%Creperies%" THEN 1 ELSE 0 END) AS [category_Creperies]
                                ,MAX(CASE WHEN A.categories like "%Cuban%" THEN 1 ELSE 0 END) AS [category_Cuban]
                                ,MAX(CASE WHEN A.categories like "%Dance Clubs%" THEN 1 ELSE 0 END) AS [category_Dance Clubs]
                                ,MAX(CASE WHEN A.categories like "%Delis%" THEN 1 ELSE 0 END) AS [category_Delis]
                                ,MAX(CASE WHEN A.categories like "%Desserts%" THEN 1 ELSE 0 END) AS [category_Desserts]
                                ,MAX(CASE WHEN A.categories like "%Dim Sum%" THEN 1 ELSE 0 END) AS [category_Dim Sum]
                                ,MAX(CASE WHEN A.categories like "%Diners%" THEN 1 ELSE 0 END) AS [category_Diners]
                                ,MAX(CASE WHEN A.categories like "%Dive Bars%" THEN 1 ELSE 0 END) AS [category_Dive Bars]
                                ,MAX(CASE WHEN A.categories like "%Donuts%" THEN 1 ELSE 0 END) AS [category_Donuts]
                                ,MAX(CASE WHEN A.categories like "%Ethiopian%" THEN 1 ELSE 0 END) AS [category_Ethiopian]
                                ,MAX(CASE WHEN A.categories like "%Ethnic Food%" THEN 1 ELSE 0 END) AS [category_Ethnic Food]
                                ,MAX(CASE WHEN A.categories like "%Event Planning & Services%" THEN 1 ELSE 0 END) AS [category_Event Planning & Services]
                                ,MAX(CASE WHEN A.categories like "%Fast Food%" THEN 1 ELSE 0 END) AS [category_Fast Food]
                                ,MAX(CASE WHEN A.categories like "%Fish & Chips%" THEN 1 ELSE 0 END) AS [category_Fish & Chips]
                                ,MAX(CASE WHEN A.categories like "%Fondue%" THEN 1 ELSE 0 END) AS [category_Fondue]
                                ,MAX(CASE WHEN A.categories like "%Food%" THEN 1 ELSE 0 END) AS [category_Food]
                                ,MAX(CASE WHEN A.categories like "%Food Court%" THEN 1 ELSE 0 END) AS [category_Food Court]
                                ,MAX(CASE WHEN A.categories like "%Food Delivery Services%" THEN 1 ELSE 0 END) AS [category_Food Delivery Services]
                                ,MAX(CASE WHEN A.categories like "%Food Trucks%" THEN 1 ELSE 0 END) AS [category_Food Trucks]
                                ,MAX(CASE WHEN A.categories like "%French%" THEN 1 ELSE 0 END) AS [category_French]
                                ,MAX(CASE WHEN A.categories like "%Fruits & Veggies%" THEN 1 ELSE 0 END) AS [category_Fruits & Veggies]
                                ,MAX(CASE WHEN A.categories like "%Gastropubs%" THEN 1 ELSE 0 END) AS [category_Gastropubs]
                                ,MAX(CASE WHEN A.categories like "%Gelato%" THEN 1 ELSE 0 END) AS [category_Gelato]
                                ,MAX(CASE WHEN A.categories like "%German%" THEN 1 ELSE 0 END) AS [category_German]
                                ,MAX(CASE WHEN A.categories like "%Gluten-Free%" THEN 1 ELSE 0 END) AS [category_Gluten-Free]
                                ,MAX(CASE WHEN A.categories like "%Golf%" THEN 1 ELSE 0 END) AS [category_Golf]
                                ,MAX(CASE WHEN A.categories like "%Greek%" THEN 1 ELSE 0 END) AS [category_Greek]
                                ,MAX(CASE WHEN A.categories like "%Grocery%" THEN 1 ELSE 0 END) AS [category_Grocery]
                                ,MAX(CASE WHEN A.categories like "%Halal%" THEN 1 ELSE 0 END) AS [category_Halal]
                                ,MAX(CASE WHEN A.categories like "%Hawaiian%" THEN 1 ELSE 0 END) AS [category_Hawaiian]
                                ,MAX(CASE WHEN A.categories like "%Hookah Bars%" THEN 1 ELSE 0 END) AS [category_Hookah Bars]
                                ,MAX(CASE WHEN A.categories like "%Hot Dogs%" THEN 1 ELSE 0 END) AS [category_Hot Dogs]
                                ,MAX(CASE WHEN A.categories like "%Hotels%" THEN 1 ELSE 0 END) AS [category_Hotels]
                                ,MAX(CASE WHEN A.categories like "%Hotels & Travel%" THEN 1 ELSE 0 END) AS [category_Hotels & Travel]
                                ,MAX(CASE WHEN A.categories like "%Ice Cream & Frozen Yogurt%" THEN 1 ELSE 0 END) AS [category_Ice Cream & Frozen Yogurt]
                                ,MAX(CASE WHEN A.categories like "%Indian%" THEN 1 ELSE 0 END) AS [category_Indian]
                                ,MAX(CASE WHEN A.categories like "%Internet Cafes%" THEN 1 ELSE 0 END) AS [category_Internet Cafes]
                                ,MAX(CASE WHEN A.categories like "%Irish%" THEN 1 ELSE 0 END) AS [category_Irish]
                                ,MAX(CASE WHEN A.categories like "%Italian%" THEN 1 ELSE 0 END) AS [category_Italian]
                                ,MAX(CASE WHEN A.categories like "%Japanese%" THEN 1 ELSE 0 END) AS [category_Japanese]
                                ,MAX(CASE WHEN A.categories like "%Jazz & Blues%" THEN 1 ELSE 0 END) AS [category_Jazz & Blues]
                                ,MAX(CASE WHEN A.categories like "%Juice Bars & Smoothies%" THEN 1 ELSE 0 END) AS [category_Juice Bars & Smoothies]
                                ,MAX(CASE WHEN A.categories like "%Karaoke%" THEN 1 ELSE 0 END) AS [category_Karaoke]
                                ,MAX(CASE WHEN A.categories like "%Korean%" THEN 1 ELSE 0 END) AS [category_Korean]
                                ,MAX(CASE WHEN A.categories like "%Kosher%" THEN 1 ELSE 0 END) AS [category_Kosher]
                                ,MAX(CASE WHEN A.categories like "%Latin American%" THEN 1 ELSE 0 END) AS [category_Latin American]
                                ,MAX(CASE WHEN A.categories like "%Lebanese%" THEN 1 ELSE 0 END) AS [category_Lebanese]
                                ,MAX(CASE WHEN A.categories like "%Live/Raw Food%" THEN 1 ELSE 0 END) AS [category_Live/Raw Food]
                                ,MAX(CASE WHEN A.categories like "%Lounges%" THEN 1 ELSE 0 END) AS [category_Lounges]
                                ,MAX(CASE WHEN A.categories like "%Mediterranean%" THEN 1 ELSE 0 END) AS [category_Mediterranean]
                                ,MAX(CASE WHEN A.categories like "%Mexican%" THEN 1 ELSE 0 END) AS [category_Mexican]
                                ,MAX(CASE WHEN A.categories like "%Middle Eastern%" THEN 1 ELSE 0 END) AS [category_Middle Eastern]
                                ,MAX(CASE WHEN A.categories like "%Modern European%" THEN 1 ELSE 0 END) AS [category_Modern European]
                                ,MAX(CASE WHEN A.categories like "%Mongolian%" THEN 1 ELSE 0 END) AS [category_Mongolian]
                                ,MAX(CASE WHEN A.categories like "%Music Venues%" THEN 1 ELSE 0 END) AS [category_Music Venues]
                                ,MAX(CASE WHEN A.categories like "%Nightlife%" THEN 1 ELSE 0 END) AS [category_Nightlife]
                                ,MAX(CASE WHEN A.categories like "%Pakistani%" THEN 1 ELSE 0 END) AS [category_Pakistani]
                                ,MAX(CASE WHEN A.categories like "%Persian/Iranian%" THEN 1 ELSE 0 END) AS [category_Persian/Iranian]
                                ,MAX(CASE WHEN A.categories like "%Pizza%" THEN 1 ELSE 0 END) AS [category_Pizza]
                                ,MAX(CASE WHEN A.categories like "%Polish%" THEN 1 ELSE 0 END) AS [category_Polish]
                                ,MAX(CASE WHEN A.categories like "%Pool Halls%" THEN 1 ELSE 0 END) AS [category_Pool Halls]
                                ,MAX(CASE WHEN A.categories like "%Pubs%" THEN 1 ELSE 0 END) AS [category_Pubs]
                                ,MAX(CASE WHEN A.categories like "%Ramen%" THEN 1 ELSE 0 END) AS [category_Ramen]
                                ,MAX(CASE WHEN A.categories like "%Salad%" THEN 1 ELSE 0 END) AS [category_Salad]
                                ,MAX(CASE WHEN A.categories like "%Sandwiches%" THEN 1 ELSE 0 END) AS [category_Sandwiches]
                                ,MAX(CASE WHEN A.categories like "%Scandinavian%" THEN 1 ELSE 0 END) AS [category_Scandinavian]
                                ,MAX(CASE WHEN A.categories like "%Seafood%" THEN 1 ELSE 0 END) AS [category_Seafood]
                                ,MAX(CASE WHEN A.categories like "%Seafood Markets%" THEN 1 ELSE 0 END) AS [category_Seafood Markets]
                                ,MAX(CASE WHEN A.categories like "%Shaved Ice%" THEN 1 ELSE 0 END) AS [category_Shaved Ice]
                                ,MAX(CASE WHEN A.categories like "%Soul Food%" THEN 1 ELSE 0 END) AS [category_Soul Food]
                                ,MAX(CASE WHEN A.categories like "%Soup%" THEN 1 ELSE 0 END) AS [category_Soup]
                                ,MAX(CASE WHEN A.categories like "%Southern%" THEN 1 ELSE 0 END) AS [category_Southern]
                                ,MAX(CASE WHEN A.categories like "%Spanish%" THEN 1 ELSE 0 END) AS [category_Spanish]
                                ,MAX(CASE WHEN A.categories like "%Specialty Food%" THEN 1 ELSE 0 END) AS [category_Specialty Food]
                                ,MAX(CASE WHEN A.categories like "%Sports Bars%" THEN 1 ELSE 0 END) AS [category_Sports Bars]
                                ,MAX(CASE WHEN A.categories like "%Steakhouses%" THEN 1 ELSE 0 END) AS [category_Steakhouses]
                                ,MAX(CASE WHEN A.categories like "%Sushi Bars%" THEN 1 ELSE 0 END) AS [category_Sushi Bars]
                                ,MAX(CASE WHEN A.categories like "%Szechuan%" THEN 1 ELSE 0 END) AS [category_Szechuan]
                                ,MAX(CASE WHEN A.categories like "%Taiwanese%" THEN 1 ELSE 0 END) AS [category_Taiwanese]
                                ,MAX(CASE WHEN A.categories like "%Tapas/Small Plates%" THEN 1 ELSE 0 END) AS [category_Tapas/Small Plates]
                                ,MAX(CASE WHEN A.categories like "%Tea Rooms%" THEN 1 ELSE 0 END) AS [category_Tea Rooms]
                                ,MAX(CASE WHEN A.categories like "%Tex-Mex%" THEN 1 ELSE 0 END) AS [category_Tex-Mex]
                                ,MAX(CASE WHEN A.categories like "%Thai%" THEN 1 ELSE 0 END) AS [category_Thai]
                                ,MAX(CASE WHEN A.categories like "%Turkish%" THEN 1 ELSE 0 END) AS [category_Turkish]
                                ,MAX(CASE WHEN A.categories like "%Vegan%" THEN 1 ELSE 0 END) AS [category_Vegan]
                                ,MAX(CASE WHEN A.categories like "%Vegetarian%" THEN 1 ELSE 0 END) AS [category_Vegetarian]
                                ,MAX(CASE WHEN A.categories like "%Venues & Event Spaces%" THEN 1 ELSE 0 END) AS [category_Venues & Event Spaces]
                                ,MAX(CASE WHEN A.categories like "%Vietnamese%" THEN 1 ELSE 0 END) AS [category_Vietnamese]
                                ,MAX(CASE WHEN A.categories like "%Wine & Spirits%" THEN 1 ELSE 0 END) AS [category_Wine & Spirits]
                                ,MAX(CASE WHEN A.categories like "%Wine Bars%" THEN 1 ELSE 0 END) AS [category_Wine Bars]
                                ,MAX(CASE WHEN A.categories like "%Wineries%" THEN 1 ELSE 0 END) AS [category_Wineries]
FROM Restaurants_Analysis_2 A
Group by A.business_id
')

###########################################
# Add restaurant related categories to fields 
###########################################

business_Analysis_1$`attributes.Accepts Credit Cards` <- as.character(business_Analysis_1$`attributes.Accepts Credit Cards`)
business_Analysis_1$`attributes.By Appointment Only` <- as.character(business_Analysis_1$`attributes.By Appointment Only`)
business_Analysis_1$`attributes.Happy Hour` <- as.character(business_Analysis_1$`attributes.Happy Hour`)
business_Analysis_1$`attributes.Accepts Credit Cards` <- as.character(business_Analysis_1$`attributes.Accepts Credit Cards`)
business_Analysis_1$`attributes.Good For Groups` <- as.character(business_Analysis_1$`attributes.Good For Groups`)
business_Analysis_1$`attributes.Outdoor Seating` <- as.character(business_Analysis_1$`attributes.Outdoor Seating`)
business_Analysis_1$`attributes.Price Range` <- as.character(business_Analysis_1$`attributes.Price Range`)
business_Analysis_1$`attributes.Good for Kids` <- as.character(business_Analysis_1$`attributes.Good for Kids`)
business_Analysis_1$`attributes.Alcohol` <- as.character(business_Analysis_1$`attributes.Alcohol`)
business_Analysis_1$`attributes.Noise Level` <- as.character(business_Analysis_1$`attributes.Noise Level`)
business_Analysis_1$`attributes.Has TV` <- as.character(business_Analysis_1$`attributes.Has TV`)
business_Analysis_1$`attributes.Attire` <- as.character(business_Analysis_1$`attributes.Attire`)
business_Analysis_1$`attributes.Good For Dancing` <- as.character(business_Analysis_1$`attributes.Good For Dancing`)
business_Analysis_1$`attributes.Delivery` <- as.character(business_Analysis_1$`attributes.Delivery`)
business_Analysis_1$`attributes.Coat Check` <- as.character(business_Analysis_1$`attributes.Coat Check`)
business_Analysis_1$`attributes.Smoking` <- as.character(business_Analysis_1$`attributes.Smoking`)
business_Analysis_1$`attributes.Take-out` <- as.character(business_Analysis_1$`attributes.Take-out`)
business_Analysis_1$`attributes.Takes Reservations` <- as.character(business_Analysis_1$`attributes.Takes Reservations`)
business_Analysis_1$`attributes.Waiter Service` <- as.character(business_Analysis_1$`attributes.Waiter Service`)
business_Analysis_1$`attributes.Wi-Fi` <- as.character(business_Analysis_1$`attributes.Wi-Fi`)
business_Analysis_1$`attributes.Caters` <- as.character(business_Analysis_1$`attributes.Caters`)
business_Analysis_1$`attributes.Drive-Thru` <- as.character(business_Analysis_1$`attributes.Drive-Thru`)
business_Analysis_1$`attributes.Wheelchair Accessible` <- as.character(business_Analysis_1$`attributes.Wheelchair Accessible`)
business_Analysis_1$`attributes.BYOB` <- as.character(business_Analysis_1$`attributes.BYOB`)
business_Analysis_1$`attributes.Corkage` <- as.character(business_Analysis_1$`attributes.Corkage`)
business_Analysis_1$`attributes.BYOB/Corkage` <- as.character(business_Analysis_1$`attributes.BYOB/Corkage`)
business_Analysis_1$`attributes.Order at Counter` <- as.character(business_Analysis_1$`attributes.Order at Counter`)
business_Analysis_1$`attributes.Dogs Allowed` <- as.character(business_Analysis_1$`attributes.Dogs Allowed`)
business_Analysis_1$`attributes.Open 24 Hours` <- as.character(business_Analysis_1$`attributes.Open 24 Hours`)
business_Analysis_1$`attributes.Accepts Insurance` <- as.character(business_Analysis_1$`attributes.Accepts Insurance`)
business_Analysis_1$`attributes.Ages Allowed` <- as.character(business_Analysis_1$`attributes.Ages Allowed`)
business_Analysis_1$`attributes.Ambience.romantic` <- as.character(business_Analysis_1$`attributes.Ambience.romantic`)
business_Analysis_1$`attributes.Ambience.intimate` <- as.character(business_Analysis_1$`attributes.Ambience.intimate`)
business_Analysis_1$`attributes.Ambience.classy` <- as.character(business_Analysis_1$`attributes.Ambience.classy`)
business_Analysis_1$`attributes.Ambience.hipster` <- as.character(business_Analysis_1$`attributes.Ambience.hipster`)
business_Analysis_1$`attributes.Ambience.divey` <- as.character(business_Analysis_1$`attributes.Ambience.divey`)
business_Analysis_1$`attributes.Ambience.touristy` <- as.character(business_Analysis_1$`attributes.Ambience.touristy`)
business_Analysis_1$`attributes.Ambience.trendy` <- as.character(business_Analysis_1$`attributes.Ambience.trendy`)
business_Analysis_1$`attributes.Ambience.upscale` <- as.character(business_Analysis_1$`attributes.Ambience.upscale`)
business_Analysis_1$`attributes.Ambience.casual` <- as.character(business_Analysis_1$`attributes.Ambience.casual`)
business_Analysis_1$`attributes.Good For.dessert` <- as.character(business_Analysis_1$`attributes.Good For.dessert`)
business_Analysis_1$`attributes.Good For.latenight` <- as.character(business_Analysis_1$`attributes.Good For.latenight`)
business_Analysis_1$`attributes.Good For.lunch` <- as.character(business_Analysis_1$`attributes.Good For.lunch`)
business_Analysis_1$`attributes.Good For.dinner` <- as.character(business_Analysis_1$`attributes.Good For.dinner`)
business_Analysis_1$`attributes.Good For.breakfast` <- as.character(business_Analysis_1$`attributes.Good For.breakfast`)
business_Analysis_1$`attributes.Good For.brunch` <- as.character(business_Analysis_1$`attributes.Good For.brunch`)
business_Analysis_1$`attributes.Parking.garage` <- as.character(business_Analysis_1$`attributes.Parking.garage`)
business_Analysis_1$`attributes.Parking.street` <- as.character(business_Analysis_1$`attributes.Parking.street`)
business_Analysis_1$`attributes.Parking.validated` <- as.character(business_Analysis_1$`attributes.Parking.validated`)
business_Analysis_1$`attributes.Parking.lot` <- as.character(business_Analysis_1$`attributes.Parking.lot`)
business_Analysis_1$`attributes.Parking.valet` <- as.character(business_Analysis_1$`attributes.Parking.valet`)
business_Analysis_1$`attributes.Music.dj` <- as.character(business_Analysis_1$`attributes.Music.dj`)
business_Analysis_1$`attributes.Music.background_music` <- as.character(business_Analysis_1$`attributes.Music.background_music`)
business_Analysis_1$`attributes.Music.karaoke` <- as.character(business_Analysis_1$`attributes.Music.karaoke`)
business_Analysis_1$`attributes.Music.live` <- as.character(business_Analysis_1$`attributes.Music.live`)
business_Analysis_1$`attributes.Music.video` <- as.character(business_Analysis_1$`attributes.Music.video`)
business_Analysis_1$`attributes.Music.jukebox` <- as.character(business_Analysis_1$`attributes.Music.jukebox`)
business_Analysis_1$`attributes.Music.playlist` <- as.character(business_Analysis_1$`attributes.Music.playlist`)
business_Analysis_1$`attributes.Hair Types Specialized In.coloring` <- as.character(business_Analysis_1$`attributes.Hair Types Specialized In.coloring`)
business_Analysis_1$`attributes.Hair Types Specialized In.africanamerican` <- as.character(business_Analysis_1$`attributes.Hair Types Specialized In.africanamerican`)
business_Analysis_1$`attributes.Hair Types Specialized In.curly` <- as.character(business_Analysis_1$`attributes.Hair Types Specialized In.curly`)
business_Analysis_1$`attributes.Hair Types Specialized In.perms` <- as.character(business_Analysis_1$`attributes.Hair Types Specialized In.perms`)
business_Analysis_1$`attributes.Hair Types Specialized In.kids` <- as.character(business_Analysis_1$`attributes.Hair Types Specialized In.kids`)
business_Analysis_1$`attributes.Hair Types Specialized In.extensions` <- as.character(business_Analysis_1$`attributes.Hair Types Specialized In.extensions`)
business_Analysis_1$`attributes.Hair Types Specialized In.asian` <- as.character(business_Analysis_1$`attributes.Hair Types Specialized In.asian`)
business_Analysis_1$`attributes.Hair Types Specialized In.straightperms` <- as.character(business_Analysis_1$`attributes.Hair Types Specialized In.straightperms`)
business_Analysis_1$`attributes.Payment Types.amex` <- as.character(business_Analysis_1$`attributes.Payment Types.amex`)
business_Analysis_1$`attributes.Payment Types.cash_only` <- as.character(business_Analysis_1$`attributes.Payment Types.cash_only`)
business_Analysis_1$`attributes.Payment Types.mastercard` <- as.character(business_Analysis_1$`attributes.Payment Types.mastercard`)
business_Analysis_1$`attributes.Payment Types.visa` <- as.character(business_Analysis_1$`attributes.Payment Types.visa`)
business_Analysis_1$`attributes.Payment Types.discover` <- as.character(business_Analysis_1$`attributes.Payment Types.discover`)
business_Analysis_1$`attributes.Dietary Restrictions.dairy-free` <- as.character(business_Analysis_1$`attributes.Dietary Restrictions.dairy-free`)
business_Analysis_1$`attributes.Dietary Restrictions.gluten-free` <- as.character(business_Analysis_1$`attributes.Dietary Restrictions.gluten-free`)
business_Analysis_1$`attributes.Dietary Restrictions.vegan` <- as.character(business_Analysis_1$`attributes.Dietary Restrictions.vegan`)
business_Analysis_1$`attributes.Dietary Restrictions.kosher` <- as.character(business_Analysis_1$`attributes.Dietary Restrictions.kosher`)
business_Analysis_1$`attributes.Dietary Restrictions.halal` <- as.character(business_Analysis_1$`attributes.Dietary Restrictions.halal`)
business_Analysis_1$`attributes.Dietary Restrictions.soy-free` <- as.character(business_Analysis_1$`attributes.Dietary Restrictions.soy-free`)
business_Analysis_1$`attributes.Dietary Restrictions.vegetarian` <- as.character(business_Analysis_1$`attributes.Dietary Restrictions.vegetarian`)

write.csv(business_Analysis_1, file = "business_Analysis_1.csv")

rm(Restaurants_Analysis_4)

Restaurants_Analysis_4  <- sqldf('  
SELECT A.business_id, A.Highly_Liked
,B.[category_Active Life]	,B.[category_Afghan]	,B.[category_American (New)]	,B.[category_American (Traditional)]	,B.[category_Arcades]	,B.[category_Arts & Entertainment]	,B.[category_Asian Fusion]	,B.[category_Automotive]	,B.[category_Bagels]	,B.[category_Bakeries]	,B.[category_Barbeque]	,B.[category_Bars]	,B.[category_Basque]	,B.[category_Beer]	,B.[category_Brazilian]	,B.[category_Breakfast & Brunch]	,B.[category_Breweries]	,B.[category_British]	,B.[category_Bubble Tea]	,B.[category_Buffets]	,B.[category_Burgers]	,B.[category_Butcher]	,B.[category_Cafes]	,B.[category_Cajun/Creole]	,B.[category_Cambodian]	,B.[category_Cantonese]	,B.[category_Car Wash]	,B.[category_Caribbean]	,B.[category_Caterers]	,B.[category_Cheesesteaks]	,B.[category_Chicken Wings]	,B.[category_Chinese]	,B.[category_Cocktail Bars]	,B.[category_Coffee & Tea]	,B.[category_Comfort Food]	,B.[category_Creperies]	,B.[category_Cuban]	,B.[category_Dance Clubs]	,B.[category_Delis]	,B.[category_Desserts]	,B.[category_Dim Sum]	,B.[category_Diners]	,B.[category_Dive Bars]	,B.[category_Donuts]	,B.[category_Ethiopian]	,B.[category_Ethnic Food]	,B.[category_Event Planning & Services]	,B.[category_Fast Food]	,B.[category_Fish & Chips]	,B.[category_Fondue]	,B.[category_Food]	,B.[category_Food Court]	,B.[category_Food Delivery Services]	,B.[category_Food Trucks]	,B.[category_French]	,B.[category_Fruits & Veggies]	,B.[category_Gastropubs]	,B.[category_Gelato]	,B.[category_German]	,B.[category_Gluten-Free]	,B.[category_Golf]	,B.[category_Greek]	,B.[category_Grocery]	,B.[category_Halal]	,B.[category_Hawaiian]	,B.[category_Hookah Bars]	,B.[category_Hot Dogs]	,B.[category_Hotels]	,B.[category_Hotels & Travel]	,B.[category_Ice Cream & Frozen Yogurt]	,B.[category_Indian]	,B.[category_Internet Cafes]	,B.[category_Irish]	,B.[category_Italian]	,B.[category_Japanese]	,B.[category_Jazz & Blues]	,B.[category_Juice Bars & Smoothies]	,B.[category_Karaoke]	,B.[category_Korean]	,B.[category_Kosher]	,B.[category_Latin American]	,B.[category_Lebanese]	,B.[category_Live/Raw Food]	,B.[category_Lounges]	,B.[category_Mediterranean]	,B.[category_Mexican]	,B.[category_Middle Eastern]	,B.[category_Modern European]	,B.[category_Mongolian]	,B.[category_Music Venues]	,B.[category_Nightlife]	,B.[category_Pakistani]	,B.[category_Persian/Iranian]	,B.[category_Pizza]	,B.[category_Polish]	,B.[category_Pool Halls]	,B.[category_Pubs]	,B.[category_Ramen]	,B.[category_Salad]	,B.[category_Sandwiches]	,B.[category_Scandinavian]	,B.[category_Seafood]	,B.[category_Seafood Markets]	,B.[category_Shaved Ice]	,B.[category_Soul Food]	,B.[category_Soup]	,B.[category_Southern]	,B.[category_Spanish]	,B.[category_Specialty Food]	,B.[category_Sports Bars]	,B.[category_Steakhouses]	,B.[category_Sushi Bars]	,B.[category_Szechuan]	,B.[category_Taiwanese]	,B.[category_Tapas/Small Plates]	,B.[category_Tea Rooms]	,B.[category_Tex-Mex]	,B.[category_Thai]	,B.[category_Turkish]	,B.[category_Vegan]	,B.[category_Vegetarian]	,B.[category_Venues & Event Spaces]	,B.[category_Vietnamese]	,B.[category_Wine & Spirits]	,B.[category_Wine Bars]	,B.[category_Wineries]
FROM Local_Businesses_Reviewed_By_Locals_avg_ratings A
LEFT JOIN Restaurants_Analysis_3 B
ON A.business_id = B.business_id
')
#128 columns

Restaurants_Analysis_5  <- sqldf('  
SELECT A.*
,C.[hours.Tuesday.close]	,C.[hours.Tuesday.open]	,C.[hours.Friday.close]	,C.[hours.Friday.open]	,C.[hours.Monday.close]	,C.[hours.Monday.open]	,C.[hours.Wednesday.close]	,C.[hours.Wednesday.open]	,C.[hours.Thursday.close]	,C.[hours.Thursday.open]	,C.[hours.Sunday.close]	,C.[hours.Sunday.open]	,C.[hours.Saturday.close]	,C.[hours.Saturday.open]	,C.[attributes.By Appointment Only]	,C.[attributes.Happy Hour]	,C.[attributes.Accepts Credit Cards]	,C.[attributes.Good For Groups]	,C.[attributes.Outdoor Seating]	,C.[attributes.Price Range]	,C.[attributes.Good for Kids]	,C.[attributes.Alcohol]	,C.[attributes.Noise Level]	,C.[attributes.Has TV]	,C.[attributes.Attire]	,C.[attributes.Good For Dancing]	,C.[attributes.Delivery]	,C.[attributes.Coat Check]	,C.[attributes.Smoking]	,C.[attributes.Take-out]	,C.[attributes.Takes Reservations]	,C.[attributes.Waiter Service]	,C.[attributes.Wi-Fi]	,C.[attributes.Caters]	,C.[attributes.Drive-Thru]	,C.[attributes.Wheelchair Accessible]	,C.[attributes.BYOB]	,C.[attributes.Corkage]	,C.[attributes.BYOB/Corkage]	,C.[attributes.Order at Counter]	,C.[attributes.Dogs Allowed]	,C.[attributes.Open 24 Hours]	,C.[attributes.Accepts Insurance]	,C.[attributes.Ages Allowed]	,C.[attributes.Ambience.romantic]	,C.[attributes.Ambience.intimate]	,C.[attributes.Ambience.classy]	,C.[attributes.Ambience.hipster]	,C.[attributes.Ambience.divey]	,C.[attributes.Ambience.touristy]	,C.[attributes.Ambience.trendy]	,C.[attributes.Ambience.upscale]	,C.[attributes.Ambience.casual]	,C.[attributes.Good For.dessert]	,C.[attributes.Good For.latenight]	,C.[attributes.Good For.lunch]	,C.[attributes.Good For.dinner]	,C.[attributes.Good For.breakfast]	,C.[attributes.Good For.brunch]	,C.[attributes.Parking.garage]	,C.[attributes.Parking.street]	,C.[attributes.Parking.validated]	,C.[attributes.Parking.lot]	,C.[attributes.Parking.valet]	,C.[attributes.Music.dj]	,C.[attributes.Music.background_music]	,C.[attributes.Music.karaoke]	,C.[attributes.Music.live]	,C.[attributes.Music.video]	,C.[attributes.Music.jukebox]	,C.[attributes.Music.playlist]	,C.[attributes.Hair Types Specialized In.coloring]	,C.[attributes.Hair Types Specialized In.africanamerican]	,C.[attributes.Hair Types Specialized In.curly]	,C.[attributes.Hair Types Specialized In.perms]	,C.[attributes.Hair Types Specialized In.kids]	,C.[attributes.Hair Types Specialized In.extensions]	,C.[attributes.Hair Types Specialized In.asian]	,C.[attributes.Hair Types Specialized In.straightperms]	,C.[attributes.Payment Types.amex]	,C.[attributes.Payment Types.cash_only]	,C.[attributes.Payment Types.mastercard]	,C.[attributes.Payment Types.visa]	,C.[attributes.Payment Types.discover]	,C.[attributes.Dietary Restrictions.dairy-free]	,C.[attributes.Dietary Restrictions.gluten-free]	,C.[attributes.Dietary Restrictions.vegan]	,C.[attributes.Dietary Restrictions.kosher]	,C.[attributes.Dietary Restrictions.halal]	,C.[attributes.Dietary Restrictions.soy-free]	,C.[attributes.Dietary Restrictions.vegetarian]
FROM Restaurants_Analysis_4 A
LEFT JOIN business_Analysis_1 C
ON A.business_id = C.business_id
')
#219 columns


###########################################
# Cut out fields that aren't well populated 
###########################################

Restaurants_Analysis_TRAIN <- Restaurants_Analysis_5

# Data Manipulation
# Remove columnss with NA > 75%
na_col <- names(Restaurants_Analysis_TRAIN[, colSums(is.na(Restaurants_Analysis_TRAIN)) > nrow(Restaurants_Analysis_TRAIN)*.75])
Restaurants_Analysis_TRAIN[,na_col] <- list(NULL)
#178 Variables
# Remove nearZeroVar fields
ZVar <- nearZeroVar(Restaurants_Analysis_TRAIN)
Restaurants_Analysis_TRAIN[,ZVar] <- list(NULL)
#54 variables
# Discart ID columns


#add column for liked back in
  #Restaurants_Analysis_TRAIN <- sqldf('  
  #SELECT A.*
  #,B.Highly_Liked
  #FROM Restaurants_Analysis_TRAIN A
  #LEFT JOIN Restaurants_Analysis_5 B
  #ON A.business_id = B.business_id
  #')

Mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}


Restaurants_Analysis_TRAIN[,1]<- list(NULL)
Restaurants_Analysis_TRAIN$Highly_Liked <- as.factor(Restaurants_Analysis_TRAIN$Highly_Liked)
Restaurants_Analysis_TRAIN[is.na(Restaurants_Analysis_TRAIN)] <- 0
write.csv(Restaurants_Analysis_TRAIN, file = "Restaurants_Analysis_TRAIN.csv")
colnames(Restaurants_Analysis_TRAIN) <- c("Highly_Liked","category_American_New"	,"category_American_Traditional"	,"category_Bars"	,"category_Breakfast_and_Brunch"	,"category_Burgers"	,"category_Chinese"	,"category_Food"	,"category_Italian"	,"category_Mexican"	,"category_Nightlife"	,"category_Pizza"	,"category_Sandwiches"	,"category_Sushi_Bars"	,"hours_Tuesday_close"	,"hours_Tuesday_open"	,"hours_Friday_close"	,"hours_Friday_open"	,"hours_Monday_close"	,"hours_Monday_open"	,"hours_Wednesday_close"	,"hours_Wednesday_open"	,"hours_Thursday_close"	,"hours_Thursday_open"	,"hours_Sunday_close"	,"hours_Sunday_open"	,"hours_Saturday_close"	,"hours_Saturday_open"	,"attributes_Good_For_Groups"	,"attributes_Outdoor_Seating"	,"attributes_Price_Range"	,"attributes_Good_for_Kids"	,"attributes_Alcohol"	,"attributes_Noise_Level"	,"attributes_Has_TV"	,"attributes_Delivery"	,"attributes_Take_out"	,"attributes_Takes_Reservations"	,"attributes_Waiter_Service"	,"attributes_Wi_Fi"	,"attributes_Caters"	,"attributes_Ambience_divey"	,"attributes_Ambience_trendy"	,"attributes_Ambience_casual"	,"attributes_Good_For_latenight"	,"attributes_Good_For_lunch"	,"attributes_Good_For_dinner"	,"attributes_Good_For_breakfast"	,"attributes_Good_For_brunch"	,"attributes_Parking_garage"	,"attributes_Parking_street"	,"attributes_Parking_lot"	,"attributes_Parking_valet")
  #53 Variables

#data cleanup for RF
  #char -> factor
  Restaurants_Analysis_TRAIN$category_American_New <- as.factor(Restaurants_Analysis_TRAIN$category_American_New)
  Restaurants_Analysis_TRAIN$category_American_Traditional <- as.factor(Restaurants_Analysis_TRAIN$category_American_Traditional)
  Restaurants_Analysis_TRAIN$category_Bars <- as.factor(Restaurants_Analysis_TRAIN$category_Bars)
  Restaurants_Analysis_TRAIN$category_Breakfast_and_Brunch <- as.factor(Restaurants_Analysis_TRAIN$category_Breakfast_and_Brunch)
  Restaurants_Analysis_TRAIN$category_Burgers <- as.factor(Restaurants_Analysis_TRAIN$category_Burgers)
  Restaurants_Analysis_TRAIN$category_Chinese <- as.factor(Restaurants_Analysis_TRAIN$category_Chinese)
  Restaurants_Analysis_TRAIN$category_Food <- as.factor(Restaurants_Analysis_TRAIN$category_Food)
  Restaurants_Analysis_TRAIN$category_Italian <- as.factor(Restaurants_Analysis_TRAIN$category_Italian)
  Restaurants_Analysis_TRAIN$category_Mexican <- as.factor(Restaurants_Analysis_TRAIN$category_Mexican)
  Restaurants_Analysis_TRAIN$category_Nightlife <- as.factor(Restaurants_Analysis_TRAIN$category_Nightlife)
  Restaurants_Analysis_TRAIN$category_Pizza <- as.factor(Restaurants_Analysis_TRAIN$category_Pizza)
  Restaurants_Analysis_TRAIN$category_Sandwiches <- as.factor(Restaurants_Analysis_TRAIN$category_Sandwiches)
  Restaurants_Analysis_TRAIN$category_Sushi_Bars <- as.factor(Restaurants_Analysis_TRAIN$category_Sushi_Bars)
  Restaurants_Analysis_TRAIN$attributes_Good_For_Groups <- as.factor(Restaurants_Analysis_TRAIN$attributes_Good_For_Groups)
  Restaurants_Analysis_TRAIN$attributes_Outdoor_Seating <- as.factor(Restaurants_Analysis_TRAIN$attributes_Outdoor_Seating)
  Restaurants_Analysis_TRAIN$attributes_Price_Range <- as.factor(Restaurants_Analysis_TRAIN$attributes_Price_Range)
  Restaurants_Analysis_TRAIN$attributes_Good_for_Kids <- as.factor(Restaurants_Analysis_TRAIN$attributes_Good_for_Kids)
  Restaurants_Analysis_TRAIN$attributes_Alcohol <- as.factor(Restaurants_Analysis_TRAIN$attributes_Alcohol)
  Restaurants_Analysis_TRAIN$attributes_Noise_Level <- as.factor(Restaurants_Analysis_TRAIN$attributes_Noise_Level)
  Restaurants_Analysis_TRAIN$attributes_Has_TV <- as.factor(Restaurants_Analysis_TRAIN$attributes_Has_TV)
  Restaurants_Analysis_TRAIN$attributes_Delivery <- as.factor(Restaurants_Analysis_TRAIN$attributes_Delivery)
  Restaurants_Analysis_TRAIN$attributes_Take_out <- as.factor(Restaurants_Analysis_TRAIN$attributes_Take_out)
  Restaurants_Analysis_TRAIN$attributes_Takes_Reservations <- as.factor(Restaurants_Analysis_TRAIN$attributes_Takes_Reservations)
  Restaurants_Analysis_TRAIN$attributes_Waiter_Service <- as.factor(Restaurants_Analysis_TRAIN$attributes_Waiter_Service)
  Restaurants_Analysis_TRAIN$attributes_Wi_Fi <- as.factor(Restaurants_Analysis_TRAIN$attributes_Wi_Fi)
  Restaurants_Analysis_TRAIN$attributes_Caters <- as.factor(Restaurants_Analysis_TRAIN$attributes_Caters)
  Restaurants_Analysis_TRAIN$attributes_Ambience_divey <- as.factor(Restaurants_Analysis_TRAIN$attributes_Ambience_divey)
  Restaurants_Analysis_TRAIN$attributes_Ambience_trendy <- as.factor(Restaurants_Analysis_TRAIN$attributes_Ambience_trendy)
  Restaurants_Analysis_TRAIN$attributes_Ambience_casual <- as.factor(Restaurants_Analysis_TRAIN$attributes_Ambience_casual)
  Restaurants_Analysis_TRAIN$attributes_Good_For_latenight <- as.factor(Restaurants_Analysis_TRAIN$attributes_Good_For_latenight)
  Restaurants_Analysis_TRAIN$attributes_Good_For_lunch <- as.factor(Restaurants_Analysis_TRAIN$attributes_Good_For_lunch)
  Restaurants_Analysis_TRAIN$attributes_Good_For_dinner <- as.factor(Restaurants_Analysis_TRAIN$attributes_Good_For_dinner)
  Restaurants_Analysis_TRAIN$attributes_Good_For_breakfast <- as.factor(Restaurants_Analysis_TRAIN$attributes_Good_For_breakfast)
  Restaurants_Analysis_TRAIN$attributes_Good_For_brunch <- as.factor(Restaurants_Analysis_TRAIN$attributes_Good_For_brunch)
  Restaurants_Analysis_TRAIN$attributes_Parking_garage <- as.factor(Restaurants_Analysis_TRAIN$attributes_Parking_garage)
  Restaurants_Analysis_TRAIN$attributes_Parking_street <- as.factor(Restaurants_Analysis_TRAIN$attributes_Parking_street)
  Restaurants_Analysis_TRAIN$attributes_Parking_lot <- as.factor(Restaurants_Analysis_TRAIN$attributes_Parking_lot)
  Restaurants_Analysis_TRAIN$attributes_Parking_valet <- as.factor(Restaurants_Analysis_TRAIN$attributes_Parking_valet)
  Restaurants_Analysis_TRAIN$Highly_Liked <- as.factor(Restaurants_Analysis_TRAIN$Highly_Liked)
  

  #0 value -> Mode
  Restaurants_Analysis_TRAIN$hours_Tuesday_close[Restaurants_Analysis_TRAIN$hours_Tuesday_close == 0] <- Mode(Restaurants_Analysis_TRAIN$hours_Tuesday_close)
  Restaurants_Analysis_TRAIN$hours_Tuesday_open[Restaurants_Analysis_TRAIN$hours_Tuesday_open == 0] <- Mode(Restaurants_Analysis_TRAIN$hours_Tuesday_open)
  Restaurants_Analysis_TRAIN$hours_Friday_close[Restaurants_Analysis_TRAIN$hours_Friday_close == 0] <- Mode(Restaurants_Analysis_TRAIN$hours_Friday_close)
  Restaurants_Analysis_TRAIN$hours_Friday_open[Restaurants_Analysis_TRAIN$hours_Friday_open == 0] <- Mode(Restaurants_Analysis_TRAIN$hours_Friday_open)
  Restaurants_Analysis_TRAIN$hours_Monday_close[Restaurants_Analysis_TRAIN$hours_Monday_close == 0] <- Mode(Restaurants_Analysis_TRAIN$hours_Monday_close)
  Restaurants_Analysis_TRAIN$hours_Monday_open[Restaurants_Analysis_TRAIN$hours_Monday_open == 0] <- Mode(Restaurants_Analysis_TRAIN$hours_Monday_open)
  Restaurants_Analysis_TRAIN$hours_Wednesday_close[Restaurants_Analysis_TRAIN$hours_Wednesday_close == 0] <- Mode(Restaurants_Analysis_TRAIN$hours_Wednesday_close)
  Restaurants_Analysis_TRAIN$hours_Wednesday_open[Restaurants_Analysis_TRAIN$hours_Wednesday_open == 0] <- Mode(Restaurants_Analysis_TRAIN$hours_Wednesday_open)
  Restaurants_Analysis_TRAIN$hours_Thursday_close[Restaurants_Analysis_TRAIN$hours_Thursday_close == 0] <- Mode(Restaurants_Analysis_TRAIN$hours_Thursday_close)
  Restaurants_Analysis_TRAIN$hours_Thursday_open[Restaurants_Analysis_TRAIN$hours_Thursday_open == 0] <- Mode(Restaurants_Analysis_TRAIN$hours_Thursday_open)
  Restaurants_Analysis_TRAIN$hours_Sunday_close[Restaurants_Analysis_TRAIN$hours_Sunday_close == 0] <- Mode(Restaurants_Analysis_TRAIN$hours_Sunday_close)
  Restaurants_Analysis_TRAIN$hours_Sunday_open[Restaurants_Analysis_TRAIN$hours_Sunday_open == 0] <- Mode(Restaurants_Analysis_TRAIN$hours_Sunday_open)
  Restaurants_Analysis_TRAIN$hours_Saturday_close[Restaurants_Analysis_TRAIN$hours_Saturday_close == 0] <- Mode(Restaurants_Analysis_TRAIN$hours_Saturday_close)
  Restaurants_Analysis_TRAIN$hours_Saturday_open[Restaurants_Analysis_TRAIN$hours_Saturday_open == 0] <- Mode(Restaurants_Analysis_TRAIN$hours_Saturday_open)
  

  #char -> number
  Restaurants_Analysis_TRAIN$hours_Tuesday_close <- as.numeric(substr(Restaurants_Analysis_TRAIN$hours_Tuesday_close,1,2))
  Restaurants_Analysis_TRAIN$hours_Tuesday_open <- as.numeric(substr(Restaurants_Analysis_TRAIN$hours_Tuesday_open,1,2))
  Restaurants_Analysis_TRAIN$hours_Friday_close <- as.numeric(substr(Restaurants_Analysis_TRAIN$hours_Friday_close,1,2))
  Restaurants_Analysis_TRAIN$hours_Friday_open <- as.numeric(substr(Restaurants_Analysis_TRAIN$hours_Friday_open,1,2))
  Restaurants_Analysis_TRAIN$hours_Monday_close <- as.numeric(substr(Restaurants_Analysis_TRAIN$hours_Monday_close,1,2))
  Restaurants_Analysis_TRAIN$hours_Monday_open <- as.numeric(substr(Restaurants_Analysis_TRAIN$hours_Monday_open,1,2))
  Restaurants_Analysis_TRAIN$hours_Wednesday_close <- as.numeric(substr(Restaurants_Analysis_TRAIN$hours_Wednesday_close,1,2))
  Restaurants_Analysis_TRAIN$hours_Wednesday_open <- as.numeric(substr(Restaurants_Analysis_TRAIN$hours_Wednesday_open,1,2))
  Restaurants_Analysis_TRAIN$hours_Thursday_close <- as.numeric(substr(Restaurants_Analysis_TRAIN$hours_Thursday_close,1,2))
  Restaurants_Analysis_TRAIN$hours_Thursday_open <- as.numeric(substr(Restaurants_Analysis_TRAIN$hours_Thursday_open,1,2))
  Restaurants_Analysis_TRAIN$hours_Sunday_close <- as.numeric(substr(Restaurants_Analysis_TRAIN$hours_Sunday_close,1,2))
  Restaurants_Analysis_TRAIN$hours_Sunday_open <- as.numeric(substr(Restaurants_Analysis_TRAIN$hours_Sunday_open,1,2))
  Restaurants_Analysis_TRAIN$hours_Saturday_close <- as.numeric(substr(Restaurants_Analysis_TRAIN$hours_Saturday_close,1,2))
  Restaurants_Analysis_TRAIN$hours_Saturday_open <- as.numeric(substr(Restaurants_Analysis_TRAIN$hours_Saturday_open,1,2))
  

###########################################
# Set up training model 
###########################################

set.seed(1900)
#set train and test
Partitions <- createDataPartition(y= Restaurants_Analysis_TRAIN$Highly_Liked, p = 0.60, list = FALSE)
model_train = Restaurants_Analysis_TRAIN[Partitions,]
model_test = Restaurants_Analysis_TRAIN[-Partitions,]

set.seed(1950)
#fit <- train(classe ~., data = model_train, method = "rf", prox = TRUE)
fit <- randomForest(Highly_Liked ~., data=model_train, ntree=5000,keep.forest=TRUE)

prediction_train <- predict(fit, model_test)

AAA <- confusionMatrix(model_test$Highly_Liked, prediction_train)

randomForest::getTree
#install.packages('party')
library(party)

