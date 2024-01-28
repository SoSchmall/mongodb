from pymongo import MongoClient
from pprint import pprint
import re
                        ################# DATABASE CONNECTION #################

# a) Authentication
client = MongoClient(
    host="127.0.0.1",
    port=27017,
    username="admin",
    password="pass",
    authSource="admin"
)

# b) Get list of databases
if "admin" in client.list_database_names():
    print("Authentication successful")
    print(client.list_database_names())
else:
    print("Authentication failed")

# c) Get list of collections
if "sample" in client.list_database_names():
    db_sample = client["sample"]
    sample_collections = db_sample.list_collection_names()
    print("Collections available in sample database:")
    print(sample_collections)
else:
    print("sample database does not exist") 

# d) Display one document
pprint(db_sample["books"].find_one())

# e) Display the number of documents
books_collection = db_sample["books"]
count_books_documents = books_collection.count_documents({})
print(f"Number of documents in collection 'books' is : {count_books_documents}")

                    ################# DATABASE EXPLORATION #################

# a) Display the number of books with more than 400 pages, then display the number of books with more than 400 pages that are published
count_400p_books = books_collection.count_documents({"pageCount": {"$gt": 400}})
count_400p_published_books = books_collection.count_documents({
    "pageCount": {"$gt": 400},
    "status": "PUBLISH"
})
print(f"Number of books with more than 400 pages: {count_400p_books}")
print(f"Number of books with more than 400 pages and published: {count_400p_published_books}")

# b) Display the number of books with the keyword "Android" in their description (short or long)
count_android_books = books_collection.count_documents({
    "$or": [
        {"shortDescription": {"$regex": "Android", "$options": "i"}},
        {"longDescription": {"$regex": "Android", "$options": "i"}}
    ]
})
print(f"Number of books with the keyword 'Android' in their description: {count_android_books}")

# c) Display 2 distinct lists of categories based on their index (0 or 1). Find the answer using a single query with "$group," "$addToSet," and "$arrayElemAt"
distinct_categories = books_collection.aggregate([
    {"$group": {"_id": None, "categories_0": {"$addToSet": {"$arrayElemAt": ["$categories", 0]}}, "categories_1": {"$addToSet": {"$arrayElemAt": ["$categories", 1]}}}}
])

for result in distinct_categories:
    distinct_categories_0 = result["categories_0"]
    distinct_categories_1 = result["categories_1"]

pprint("Distinct categories at index 0:")
pprint(distinct_categories_0)
pprint("\nDistinct categories at index 1:")
pprint(distinct_categories_1)

# d) Display the number of books that contain the following programming language names in their long description: Python, Java, C++, Scala
count_proglang_books = books_collection.count_documents({
    "longDescription": {
        "$regex": "Python|Java|C\+\+|Scala",
        "$options": "i"
    }
})
print(f"Number of books containing programming languages in their long description: {count_proglang_books}")

# e) Display various statistical information about our database: maximum, minimum, and average number of pages per category. Use an aggregation pipeline, the "$group" keyword, and appropriate accumulators
aggregation_pipeline = [
    {"$unwind": "$categories"},
    {"$group": {
        "_id": "$categories",
        "maxPages": {"$max": "$pageCount"},
        "minPages": {"$min": "$pageCount"},
        "avgPages": {"$avg": "$pageCount"}
    }}
]
category_statistics = list(books_collection.aggregate(aggregation_pipeline))

print("Display our category statistics:")
for category_stat in category_statistics:
    category = category_stat["_id"]
    max_pages = category_stat["maxPages"]
    min_pages = category_stat["minPages"]
    avg_pages = category_stat["avgPages"]
    print(f"Category: {category}")
    print(f"Max Pages: {max_pages}")
    print(f"Min Pages: {min_pages}")
    print(f"Avg Pages: {avg_pages}")

# f) Using an aggregation pipeline, create new variables by extracting information from the "dates" attribute: year, month, day. Add a condition to filter only books published after 2009. Display only the first 20 results

aggregation_pipeline_question_f = [
    {
        "$match": {
            "$expr": {
                "$gt": [
                    {"$year": "$publishedDate"},
                    2009
                ]
            }
        }
    },
    {
        "$project": {
            "_id": 0,
            "title": 1,
            "year": {"$year": "$publishedDate"},
            "month": {"$month": "$publishedDate"},
            "day": {"$dayOfMonth": "$publishedDate"}
        }
    },
    {
        "$limit": 20
    }
]

result = list(books_collection.aggregate(aggregation_pipeline_question_f))

print("Books after 2009:")
for book in result:
    pprint(book)

# g) From the list of authors, create new attributes (author1, author2 ... author_n). Observe the behavior of "$arrayElemAt". Display only the first 20 in chronological order

g_question = [
    {
        '$addFields': {
            'author1': {'$arrayElemAt': ['$authors', 0]},
            'author2': {'$arrayElemAt': ['$authors', 1]},
            'author3': {'$arrayElemAt': ['$authors', 2]}
        }
    },
    {
        '$project': {
            'author1': 1,
            'author2': 1,
            'author3': 1,
            '_id': 0
        }
    },
    {
        '$limit': 20
    }
]

result = list(db_sample['books'].aggregate(g_question))
pprint(result)

# h)  Building upon the previous query, create a column containing the name of the first author, then aggregate based on this column to obtain the number of articles for each first author. Display the number of publications for the top 10 most prolific first authors

published_10_first_authors = [
    {
        '$addFields': {
            'firstAuthor': {'$arrayElemAt': ['$authors', 0]}
        }
    },
    {
        '$group': {
            '_id': '$firstAuthor',
            'count': {'$sum': 1}
        }
    },
    {
        '$sort': {'count': -1}
    },
    {
        '$limit': 10
    }
]

result = list(db_sample['books'].aggregate(published_10_first_authors))
pprint(result)






































































