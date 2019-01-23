
import requests
import json


ORG_ID = 3
SYS_ID = 5
token = '6YjkL556gObDoFb70hp5mubt5706203374631480'
CMDB_URL = "http://172.20.238.127:9601/ci/2/271"
Alert_URL = "http://172.20.238.127:5001/cmdb/relationship/overwrite?token=6YjkL556gObDoFb70hp5mubt5706203374631480"
headers = {'Content-type': 'application/json'}


#stage1: query service_id
#####################################################################################################################################
# load service data from file
with open('/Users/sonic/Desktop/11111111/dep.json.23','r') as f:
    mhost_services = json.load(f)

# put all service in a list
service_list = []
for srv_list in mhost_services.values():
    for service in srv_list:
        service_list.append(service)
service_list = list(set(service_list))
print service_list

# query service_id by sent post request
service_id_dic = {}
for service in service_list:
    resp = requests.post(CMDB_URL + "/search2", json={"type": "SoftwareGroup","filter": {"name": service}}, headers=headers)
    if resp.status_code == 200 or resp.status_code == 204:
        resp = json.loads(resp.content)
        if len(resp):
            service_id = resp[0]['id']
            service_id_dic[str(service)] = str(service_id)
            print service_id
    else:
        print "query service_id error!"


# stage2: delete service_id
#######################################################################################################################################
for service,service_id in service_id_dic.items():
    delete_url = 'http://172.20.238.127:5001/cmdb/agent/service?id=' + str(service_id) + '&token=' + str(token)
    print delete_url
    resp = requests.delete(delete_url)
    if resp.status_code == 200 or resp.status_code == 204:
        print 'successfully delete service from CMDB : ' + str(service)
    else:
        print 'delete error : ' + str(service)


# stage3: create new service
#######################################################################################################################################
for service_name in service_id_dic.keys():
    resp  = requests.post(CMDB_URL, json={
        "key": str(service_name) + "_linux",
        "type": "SoftwareGroup",
        "attributes": [
            {
                "name": "platform",
                "value": "linux"
            }, {
                "name": "name",
                "value": str(service_name)
            }
        ]
    }, headers=headers)
    if resp.status_code == 200 or resp.status_code == 204:
        print "successfully create new service : test-svc"
    else:
        print "create service error: test-svc"


#stage4: query new service_id & host_id
########################################################################################################################################
srv_id_dic = {}
for service_name in service_id_dic.keys():
    resp = requests.post(CMDB_URL + "/search2", json={
                "type": "SoftwareGroup",
                "filter": {
                    "name": str(service_name)
                }
            }, headers=headers)
    if resp.status_code == 200 or resp.status_code == 204:
        resp = json.loads(resp.content)
        if len(resp):
            service_id = resp[0]['id']
            srv_id_dic[service_name] = str(service_id)
            print "service_id: " + str(service_id)
    else:
        print "query service_id error"

host_id_dic = {}
for host in mhost_services.keys():
    resp = requests.post(CMDB_URL + "/search2", json={
        "type": "Host",
        "filter": {
            "hostname": str(host)
        }
    }, headers=headers)
    if resp.status_code == 200 or resp.status_code == 204:
        resp = json.loads(resp.content)
        if len(resp):
            host_id = resp[0]['id']
            host_id_dic[host] = str(host_id)
            print "host_id: " + str(host_id)
    else:
        print "query host_id error"


# stage5: create new relationship for host & service
##################################################################################################################################
relationship_create_url = "http://172.20.238.127:5001/cmdb/relationship/overwrite?token=6YjkL556gObDoFb70hp5mubt5706203374631480"

for host,srv_list in mhost_services.values():
    for service in srv_list:
        host_id = host_id_dic[host]
        service_id = srv_id_dic[service]
        resp = requests.post(relationship_create_url, json={
            "op": "Create",
            "sourceId": str(host_id),
            "targetId": str(service_id),
            "type": "Member_of"
        }, headers=headers)
        if resp.status_code == 200 or resp.status_code == 204:
            print "successfully create relationship"
        else:
            print "faild to create relationship"




# # stage2: delete service_id
# for service,service_id in srv_id_dic.items():
#     delete_url = 'http://192.168.1.204:5001/cmdb/agent/service?id=' + str(service_id) + '&token=' + str(token)
#     print delete_url
#     resp = requests.delete(delete_url)
#     if resp.status_code == 200 or resp.status_code == 204:
#         print 'successfully delete service from CMDB : ' + str(service)
#     else:
#         print 'delete error : ' + str(service)



"""
# stage create new service
service_name = 'test-service'
create_url = 'http://192.168.1.204:5001/cmdb/relationship/overwrite?token=' + str(token)
#http://192.168.1.204:5001/cmdb/relationship/overwrite?token=20nTPlvoytgwsvsmrklcxgetwovxpdptsoxexwuq
print create_url
resp  = requests.post(CMDB_URL, json={
            "key": str(service_name) + "_linux",
            "type": "SoftwareGroup",
            "attributes": [
                {
                    "name": "platform",
                    "value": "linux"
                }, {
                    "name": "name",
                    "value": str(service_name)
                }
            ]
        }, headers=headers)
if resp.status_code == 200 or resp.status_code == 204:
    print "successfully create new service : test-svc"
else:
    print "create service error: test-svc"
"""


"""
# query service_id & host_id
service_name = "test-service"
resp = requests.post(CMDB_URL + "/search2", json={
            "type": "SoftwareGroup",
            "filter": {
                "name": str(service_name)
            }
        }, headers=headers)
if resp.status_code == 200 or resp.status_code == 204:
    resp = json.loads(resp.content)
    if len(resp):
        service_id = resp[0]['id']
        print "service_id: " + str(service_id)
else:
    print "query service_id error"


host = "172.18.39.88"
resp = requests.post(CMDB_URL + "/search2", json={
    "type": "Host",
    "filter": {
        "hostname": str(host)
    }
}, headers=headers)
if resp.status_code == 200 or resp.status_code == 204:
    resp = json.loads(resp.content)
    if len(resp):
        host_id = resp[0]['id']
        print "host_id: " + str(host_id)
else:
    print "query host_id error"
"""

"""
# stage create new relationship for host & service
service_id = "427580"
host_id = "427086"
relationship_create_url = "http://192.168.1.204:5001/cmdb/relationship/overwrite?token=20nTPlvoytgwsvsmrklcxgetwovxpdptsoxexwuq"

resp = requests.post(relationship_create_url, json={
            "op": "Create",
            "sourceId": str(host_id),
            "targetId": str(service_id),
            "type": "Member_of"
        }, headers=headers)
if resp.status_code == 200 or resp.status_code == 204:
    print "successfully create relationship"
else:
    print "faild to create relationship"
"""

"""
create_url = 'http://192.168.1.204:5001/cmdb/relationship/overwrite?token=' + str(token)
#http://192.168.1.204:5001/cmdb/relationship/overwrite?token=20nTPlvoytgwsvsmrklcxgetwovxpdptsoxexwuq
print create_url
res = requests.post(create_url, json={
                "op": "Create",
                "sourceId": "172.18.0.184",
                "targetId": "bc.service.billcancel",
                "type": "Member_of"
            }, headers=headers)

if res.status_code == 200 or res.status_code == 204:
    print "successfully create new host-service relationship for service: test-svc"
else:
    print res.status_code
"""


"""
#stage 1
# load service data from file
with open('/Users/sonic/Desktop/11111111/dep.json.23','r') as f:
    mhost_services = json.load(f)

# put all service in a list
service_list = []
for srv_list in mhost_services.values():
    for service in srv_list:
        service_list.append(service)
service_list = list(set(service_list))
print service_list

# query service_id by sent post request
for service in service_list
resp = requests.post(CMDB_URL + "/search2", json={"type": "SoftwareGroup","filter": {"name": service}}, headers=headers)
if resp.status_code == 200 or resp.status_code == 204:
    resp = json.loads(resp.content)
    if len(resp):
        service_id = resp[0]['id']
        print service_id
else:
    print resp
"""
