import pickle
import os
import task
import json

THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))


def rcollect_users():
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
                            users.append(advert['user'])
                    f.close()
    with open(os.path.join(THIS_FOLDER, 'users.json'), 'wb') as f:
        s = json.dumps(users, sort_keys=True, indent=4, ensure_ascii=False).encode('utf8')
        f.write(s)


if __name__ == "__main__":
    rcollect_users()
