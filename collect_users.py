import pickle
import os
import task
import json

THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))


def collect_users():

    def find(lst, key, val):
        for i, dic in enumerate(lst):
            if dic[key] == val:
                return i
        return -1

    def user_serializer(user):
        user['rubrics'] = list(user['rubrics'])
        return user

    users = list()
    for cat, _ in task.categories:
        files_list = os.listdir(os.path.join(THIS_FOLDER, 'categories', cat))
        for file in files_list:
            filename = os.path.join(THIS_FOLDER, 'categories', cat, file)
            if os.path.isfile(filename):
                with open(filename, 'rb') as f:
                    adverts = pickle.load(f)
                    for advert in adverts:
                        if isinstance(advert['user'], dict):
                            newdict = advert['user']
                            newdict['rubrics'] = {advert['category'], }
                            newdict['amountOfAds'] = advert['price'] if advert['price'] else 0
                            users.append(advert['user'])
                        elif advert['price']:
                            index = find(users, 'name', advert['user'])
                            if index >= 0:
                                users[index]['amountOfAds'] += advert['price']
                                users[index]['rubrics'].add(advert['category'])
                    f.close()
    serializable_users = [user_serializer(user) for user in users]
    with open(os.path.join(THIS_FOLDER, 'users.json'), 'wb') as f:
        s = json.dumps(serializable_users, sort_keys=True, indent=4, ensure_ascii=False).encode('utf8')
        f.write(s)


if __name__ == "__main__":
    collect_users()
