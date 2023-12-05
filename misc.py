import json

from pandas import DataFrame

l = [1, 2, 3, 4]
# save list as json with name as test
with open("test", "w") as fp:
    json.dump(l, fp)

# # load json as list
with open("test", "r") as fp:
    b = json.load(fp)


# convert class model output to df
user = DataFrame([o.__dict__ for o in data])


# save dictionary
import pickle

with open('saved_dictionary.pkl', 'wb') as f:
    pickle.dump(dictionary, f)

with open('saved_dictionary.pkl', 'rb') as f:
    loaded_dict = pickle.load(f)