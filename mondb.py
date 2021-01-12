import pymongo
from pprint import pprint

class Connexion:

    @classmethod
    def connect(cls):
        cls.user = "adra"
        cls.password = "mongodbadra"
        cls.database = "dblt"
        return pymongo.MongoClient(f"mongodb+srv://{cls.user}:{cls.password}@adra.gjziu.mongodb.net/{cls.database}?retryWrites=true&w=majority")


    @classmethod
    def open_db(cls):
        cls.client = cls.connect()
        cls.publis = cls.client.dblt.publis

    @classmethod
    def close_db(cls):
        cls.client.close()

    @classmethod
    def count_doc(cls):
        cls.open_db()
        show = cls.publis.count()
        cls.close_db()
        return show

    @classmethod
    def book(cls):
        cls.open_db()
        show = list(cls.publis.find({"type":"Book"},{"title":1, "_id":0}))
        cls.close_db()
        return show

    @classmethod
    def book_2014(cls):
        cls.open_db()
        show = list(cls.publis.find({"type":"Book", "year":{"$gt": 2014}},{"title":1, "_id":0}))
        cls.close_db()
        return show

    @classmethod
    def get_toru(cls):
        cls.open_db()
        show = list(cls.publis.find({"authors":"Toru Ishida"}))
        cls.close_db()
        return show

    @classmethod
    def get_authors(cls):
        cls.open_db()
        show = list(cls.publis.distinct("authors"))
        cls.close_db()
        return show

    @classmethod
    def tri_toru(cls):
        cls.open_db()
        show = list(cls.publis.find({"authors" : "Toru Ishida"}, {"title" : 1, "_id":0}).sort("title", 1))
        cls.close_db()
        return show

    @classmethod
    def number_toru(cls):
        cls.open_db()
        show = cls.publis.count({"authors" : "Toru Ishida"})
        cls.close_db()
        return show

    @classmethod
    def publis_toru_2011(cls):
        cls.open_db()
        show = list(cls.publis.aggregate([{"$match":{"year":{"$gte" : 2011}}}, {"$group":{"_id":"$type", "total":{ "$sum" : 1}}}]))
        cls.close_db()
        return show

    @classmethod
    def publis_by_authors(cls):
        cls.open_db()
        show = list(cls.publis.aggregate([{"$unwind": "$authors"}, {"$group":{"_id": "$authors", "number":{"$sum": 1}}}, {"$sort":{"number": 1}}]))
        cls.close_db()
        return show

#pprint(f'La collection compte {Connexion.count_doc()} documents')
#pprint(Connexion.book())
#pprint(Connexion.book_2014())
#pprint(Connexion.get_toru())
#pprint(Connexion.get_authors())
#pprint(Connexion.tri_toru())
#pprint(Connexion.number_toru())
#pprint(Connexion.publis_toru_2011())
#pprint(Connexion.publis_by_authors())
