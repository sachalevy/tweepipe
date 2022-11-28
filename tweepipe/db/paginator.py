import pymongo


class CollectionPaginator:
    def __init__(
        self,
        collection: pymongo.collection.Collection,
        projection: dict = None,
        sort: list = None,
        batch_size: int = 2048,
    ):
        self.projection = projection
        self.sort = sort
        self.batch_size = batch_size
        self.collection = collection

    def __iter__(
        self,
    ):
        sort = self.sort if self.sort else [("_id", pymongo.ASCENDING)]
        self.cursor = self.collection.find(
            projection=self.projection,
            batch_size=self.batch_size,
            sort=sort,
            allow_disk_use=True,
        )

        return self.cursor

    def __next__(
        self,
    ):
        return next(self.cursor)
