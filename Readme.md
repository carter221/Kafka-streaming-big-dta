Commande pour le producer
===============================================
python kafka-producer-api.py --lat 37.1028 --lon -8.6742

Commandde pour lancer le pyspark avec les packages necessaires
===============================================
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.2 weather_stream.py

Ici je consomme le topic weather_transformed pour voir les messages transformés