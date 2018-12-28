import json

class Data:

    def __init__(self):
        try:
            with open('./data.json') as json_file:
                self.data = json.load(json_file)
        except IOError:
            self.data = {}

    def Load(self, key):
        if self.data[key] is not None:
            return self.data[key]
        return None

    def Save(self, key, value):
        self.data[key] = value
        self.write_data()

    def Exists(self, key):
        return key in self.data

    def write_data(self):
        with open('./data.json', 'w') as outfile:
            json.dump(self.data, outfile)
