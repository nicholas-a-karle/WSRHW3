#-------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #3
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
import pymongo

DB_NAME = "documents"
DB_HOST = "localhost"
PORT = 27017
USERNAME = None
PASSWORD = None

def connectDataBase():

    # Create a database connection object using pymongo
    # --> add your Python code here
    try:
        client = pymongo.MongoClient(f"mongodb://{USERNAME}:{PASSWORD}@{DB_HOST}:{PORT}")
        return client[DB_NAME]
    except pymongo.errors.ConnectionFaluire as e:
        print("Connection failed:", e)
        raise

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary indexed by term to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here
    termCounts = {}
    for term in docText.lower().split():
        termCounts[term] = termCounts.get(term, 0) + 1

    # create a list of objects to include full term objects. [{"term", count, num_char}]
    # --> add your Python code here
    terms = []
    for term, count in termCounts.items():
        terms.append({"term": term, "count": count, "num_char": len(term)})

    # produce a final document as a dictionary including all the required document fields
    # --> add your Python code 
    document = {
        "_id": docId,
        "title": docTitle,
        "text": docText,
        "date": docDate,
        "category": docCat,
        "terms": terms
    }

    # insert the document
    # --> add your Python code here
    col.insert_one(document)

def deleteDocument(col, docId):

    # Delete the document from the database
    # --> add your Python code here
    col.delete_one({"_id": docId})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    # --> add your Python code here
    col.delete_one({"_id": docId})

    # Create the document with the same id
    # --> add your Python code here
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here
    index = {}
    for document in col.find():
        for term in document.get("terms", []):
            data = index.get(term, {"documents": []})
            data["documents"].append({"doc_id": document["_id"], "count": 1})
            index[term] = data
    return index




