#!/usr/bin/env python
import sys
import psycopg2
import psycopg2.extras
import requests
import json
import re
import getopt
import logging
from settings import config

logging.basicConfig(
    format='%(asctime)s:%(levelname)s:%(message)s', filename='/tmp/fullsync.log',
    datefmt='%Y-%m-%d %I:%M:%S', level=logging.DEBUG
)
cmd = sys.argv[1:]
opts, args = getopt.getopt(
    cmd, 'l:af',
    ['id-list', 'all-districts', 'force-sync'])
query_string = (
    "includeDescendants=true&"
    "fields=id,name,parent[id,name,href],dataSets[id],organisationUnitGroups[id]"
    "&filter=level:eq:5&paging=false")

district_id_list = ""
for option, parameter in opts:
    if option == '-l':
        district_id_list = parameter

dhis2_ids = []
if district_id_list:
    for dhis2id in district_id_list.split(','):
        dhis2_ids.append(dhis2id)


user = config["dhis2_user"]
passwd = config["dhis2_passwd"]

conn = psycopg2.connect(
    "dbname=" + config["dbname"] + " host= " + config["dbhost"] + " port=" + config["dbport"] +
    " user=" + config["dbuser"] + " password=" + config["dbpasswd"])

cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)


def get_url(url, payload={}):
    res = requests.get(url, params=payload, auth=(user, passwd))
    return res.text


def get_facility_details(facilityJson):
    is_033b = False
    level = ""
    owner = ""
    # parent = facilityJson["parent"]["name"].replace('Subcounty', '').strip()
    parent = re.sub(
        'Subcounty.*$|Sub\ County.*$', "", facilityJson["parent"]["name"],
        flags=re.IGNORECASE).strip()
    district_url = "%s/%s.json?fields=id,name,parent[id,name]" % (config["orgunits_url"], facilityJson["parent"]["id"])
    print district_url
    districtJson = get_url(district_url)
    # print districtJson
    # district = json.loads(districtJson)["parent"]["name"].replace('District', '').strip()
    district = re.sub(
        'District.*$', "",
        json.loads(districtJson)["parent"]["name"], flags=re.IGNORECASE).strip()

    orgunitGroups = facilityJson["organisationUnitGroups"]
    orgunitGroupsIds = ["%s" % k["id"] for k in orgunitGroups]
    for k, v in config["levels"].iteritems():
        if k in orgunitGroupsIds:
            level = v
    for k, v in config["owners"].iteritems():
        if k in orgunitGroupsIds:
            owner = v

    dataSets = facilityJson["dataSets"]
    dataSetsIds = ["%s" % k["id"] for k in dataSets]
    if getattr(config, "hmis_033b_id", "V1kJRs8CtW4") in dataSetsIds:
        is_033b = True
    # we return tuple (Subcounty, District, Level, is033B)
    return parent, district, level, is_033b, owner

if not dhis2_ids:
    cur.execute("SELECT dhis2id FROM districts")
    res = cur.fetchall()
    for district in res:
        dhis2_ids.append(district['dhis2id'])

for dhis2id in dhis2_ids:
    URL = "%s/%s.json?%s" % (config["orgunits_url"], dhis2id, query_string)
    print URL
    orgunits = []
    try:
        response = get_url(URL)
        orgunits_dict = json.loads(response)
        orgunits = orgunits_dict['organisationUnits']
    except:
        logging.error("E02: Sync Service failed")
        # just keep quiet for now

    for orgunit in orgunits:
        subcounty, district, level, is_033b, owner = get_facility_details(orgunit)
        if not level:
            continue
        sync_params = {
            'username': config["sync_user"], 'password': config["sync_passwd"],
            'name': orgunit["name"],
            'dhis2id': orgunit["id"], 'ftype': level, 'district': district,
            'subcounty': subcounty, 'is_033b': is_033b, 'owner': owner
        }
        try:
            resp = get_url(config["sync_url"], sync_params)
            print "Sync Service: %s" % resp
        except:
            print "Sync Service failed for:%s" % orgunit["id"]
            logging.error("E03: Sync Service failed for:%s" % orgunit["id"])
