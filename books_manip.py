from pymongo import MongoClient
from pprint import pprint
import re
                        ################# DATABASE CONNECTION #################

# Question a)
client = MongoClient(
    host="127.0.0.1",
    port=27017,
    username="admin",
    password="pass",
    authSource="admin"
)

# Question b): authentification checking. check admin because it has root role
if "admin" in client.list_database_names():
    print("Authentication successful")
    print(client.list_database_names())
else:
    print("Authentication failed")

# Question c)
if "sample" in client.list_database_names():
    db_sample = client["sample"]
    sample_collections = db_sample.list_collection_names()
    print("Collections available in sample database:")
    print(sample_collections)
else:
    print("sample database does not exist") 

# Question d)
pprint(db_sample["books"].find_one())

# Question e)
books_collection = db_sample["books"]
count_books_documents = books_collection.count_documents({})
print(f"Number of documents in collection 'books' is : {count_books_documents}")

                    ################# DATABASE EXPLORATION #################

# Question a)
count_400p_books = books_collection.count_documents({"pageCount": {"$gt": 400}})
count_400p_published_books = books_collection.count_documents({
    "pageCount": {"$gt": 400},
    "status": "PUBLISH"
})
print(f"Number of books with more than 400 pages: {count_400p_books}")
print(f"Number of books with more than 400 pages and published: {count_400p_published_books}")

# Question b)
count_android_books = books_collection.count_documents({
    "$or": [
        {"shortDescription": {"$regex": "Android", "$options": "i"}},
        {"longDescription": {"$regex": "Android", "$options": "i"}}
    ]
})
print(f"Number of books with the keyword 'Android' in their description: {count_android_books}")

# Question c)

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

# Question d)

count_proglang_books = books_collection.count_documents({
    "longDescription": {
        "$regex": "Python|Java|C\+\+|Scala",
        "$options": "i"
    }
})
print(f"Number of books containing programming languages in their long description: {count_proglang_books}")

# Question e)
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

# Question f)

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

# Question g)

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

# Question h)

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






































































