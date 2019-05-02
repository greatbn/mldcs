from pymongo import MongoClient


class MongoDBWrapper(object):
    def __init__(self):

        self.conn = MongoClient()
        self.db = self.conn.mldcs

    def save_flow(self, data=None):
        try:
            if not self.db.flows.find_one(data):
                self.db.flows.save(data)
            return True
        except Exception as e:
            print("Cannot insert flow {}".format(str(e)))
            return False
    
    def save_datapath(self, datapath_id, address):
        try:
            data = self.db.datapath.find_one({
                'datapath_id': datapath_id
            })
            if not data:
                self.db.datapath.save({
                    'datapath_id': datapath_id,
                    'address': address
                })
            return True
        except Exception as e:
            print("Cannot insert datapath {}".format(str(e)))
            return False
        
    def remove_datapath(self, datapath_id):
        try:
            self.db.datapath.remove({
                'datapath_id': datapath_id
            })
            return True
        except Exception as e:
            print ("Cannot remove datapath {} in database {}".format(datapath_id, str(e)))
            return False

    def check_exist_flow(self, datapath_id, table_id, match):
        query = {
            'datapath_id': datapath_id,
            'table_id': table_id,
            'match': match,
        }
        # Check if priority change
        result = list(self.db.flows.find(query))
        if len(result) > 0:
            return result
        else:
            return False
        # if len(result) > 0:
        #     # checking here
        #     if result[0]['priority'] != priority and result[0]['actions'] == actions:
        #         print("The priority of the flow {} in datapath {} is changed".format(
        #             result[0]['_id'], datapath_id
        #         ))
        #     if result[0]['actions'] != actions and result[0]['priority'] == priority:
        #         print("The actions of the flow {} in datapath {} is changed".format(
        #             result[0]['_id'], datapath_id
        #         ))
        #     if result[0]['actions'] != actions and result[0]['priority'] != priority:
        #         print("The actions and priority of the flow {} in datapath {} is changed".format(
        #             result[0]['_id'], datapath_id
        #         ))
        #     print("Flow exist")
        # else:
        #     print("Flow not exist")