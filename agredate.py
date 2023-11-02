import pymongo
from datetime import datetime
from datetime import datetime, timedelta


client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["sampleDB"]
collection = db["sample_collection"]


from datetime import datetime, timedelta

def aggregate(dt_from, dt_upto, group_type):
    dataset = []
    labels = []
    answer = {}

    def convert_date(year, month, day=1, hour=0, minute=0, second=0):
        return datetime(year, month, day, hour, minute, second)

    start_date = datetime.fromisoformat(dt_from)
    end_date = datetime.fromisoformat(dt_upto)

    spisok = [
        {
            "$match": {
                "dt": {"$gte": start_date, "$lte": end_date}
            }
        }
    ]

    # Сортировка по дате перед агрегацией
    spisok.append(
        {
            "$sort": {
                "dt": 1
            }
        }
    )

    if group_type == "hour":
        spisok.append(
            {
                "$group": {
                    "_id": {
                        "year": {"$year": "$dt"},
                        "month": {"$month": "$dt"},
                        "day": {"$dayOfMonth": "$dt"},
                        "hour": {"$hour": "$dt"}
                    },
                    "totalValue": {
                        "$sum": "$value"
                    }
                }
            }
        )
    elif group_type == "day":
        spisok.append(
            {
                "$group": {
                    "_id": {
                        "year": {"$year": "$dt"},
                        "month": {"$month": "$dt"},
                        "day": {"$dayOfMonth": "$dt"}
                    },
                    "totalValue": {
                        "$sum": "$value"
                    }
                }
            }
        )
    elif group_type == "month":
        spisok.append(
            {
                "$group": {
                    "_id": {
                        "year": {"$year": "$dt"},
                        "month": {"$month": "$dt"}
                    },
                    "totalValue": {
                        "$sum": "$value"
                    }
                }
            }
        )

    result = list(collection.aggregate(spisok))

    date_dict = {}
    current_date = start_date

    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        day = current_date.day

        if group_type == "hour":
            hour = current_date.hour
            date = convert_date(year, month, day, hour)
            current_date += timedelta(hours=1)
        elif group_type == "day":
            date = convert_date(year, month, day)
            current_date += timedelta(days=1)
        else:
            date = convert_date(year, month)
            current_date += timedelta(days=30)

        date_dict[date] = 0

    for item in result:
        year = item["_id"]["year"]
        month = item["_id"]["month"]
        day = item["_id"].get("day", 1)

        if group_type == "hour":
            hour = item["_id"].get("hour", 0)
            date = convert_date(year, month, day, hour)
        elif group_type == "day":
            date = convert_date(year, month, day)
        else:
            date = convert_date(year, month)

        date_dict[date] = item["totalValue"]

    for date, value in sorted(date_dict.items()):
        labels.append(date)
        dataset.append(value)

    labels = [date.isoformat() for date in labels]

    answer["dataset"] = dataset
    answer["labels"] = labels

    return answer
