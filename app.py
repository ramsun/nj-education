from flask import Flask, render_template, request, jsonify, send_from_directory
import pymongo, csv, json, random, os
from scraper import init_db


#################################################
# Database Setup
#################################################
conn = 'mongodb://localhost:27017'
client = pymongo.MongoClient(conn)
dbnames = client.list_database_names()


# Only perform the ELT process if the database does not exist
if 'education_data' in dbnames:
    print("db already exists")
    db = client.education_data
else:
    print("Populating db")
    db = client.education_data
    init_db(db)


app = Flask(__name__)


#################################################
# Flask Routes
#################################################
@app.route('/')
def process():
    return render_template("index.html")

# Shows metadata for a querried year
@app.route("/data")
def stream_data():    
    #  Make a query to mongo
    mycol = db["education_data"]
    cursor = mycol.find()

    # Convert the cursor object to a list of dictionaries 
    data = []
    for document in cursor:
        # pop the _id, since it is an ObjectId class (not serializeable into a json) 
        document.pop("_id")
        data.append(document)
    
    # Return the "data" object as a json
    return jsonify(data)


if __name__ == "__main__":
	app.run(debug=True)