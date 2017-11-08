#!/usr/bin/python
# Author: Samuel Sekiwere <sekiskylink@gmail.com>

import os
import sys
import web
import urllib
import logging
# import requests
# import parsedatetime
from web.contrib.template import render_jinja
from settings import config

filedir = os.path.dirname(__file__)
sys.path.append(os.path.join(filedir))
# from pagination import doquery, getPaginationString, countquery

# cal = parsedatetime.Calendar()


class AppURLopener(urllib.FancyURLopener):
    version = "interapp /1.0"

urllib._urlopener = AppURLopener()

logging.basicConfig(
    format='%(asctime)s:%(levelname)s:%(message)s', filename='/tmp/fsync_web.log',
    datefmt='%Y-%m-%d %I:%M:%S', level=logging.DEBUG
)

urls = (
    "/", "Index",
    "/create", "CreateFacility",
)

# web.config.smtp_server = 'mail.mydomain.com'
web.config.debug = False

app = web.application(urls, globals())
db = web.database(
    dbn='postgres',
    user=config["dbuser"],
    pw=config["dbpasswd"],
    db=config["dbname"],
    host=config["dbhost"]
)

db2 = web.database(
    dbn='postgres',
    user=config["mtrac_dbuser"],
    pw=config["mtrac_dbpasswd"],
    db=config["mtrac_dbname"],
    host=config["mtrac_dbhost"]
)
store = web.session.DBStore(db, 'sessions')
session = web.session.Session(app, store, initializer={'loggedin': False})

render = render_jinja(
    'templates',
    encoding='utf-8'
)
render._lookup.globals.update(
    ses=session
)

SETTINGS = {
    'PAGE_LIMIT': 25,
}


def lit(**keywords):
    return keywords


def default(*args):
    p = [i for i in args if i or i == 0]
    if p.__len__():
        return p[0]
    if args.__len__():
        return args[args.__len__() - 1]
    return None


def auth_user(db, username, password):
    sql = (
        "SELECT id,firstname,lastname FROM users WHERE username = '%s' AND password = "
        "crypt('%s', password)")
    res = db.query(sql % (username, password))
    if not res:
        return False, "Wrong username or password"
    else:
        return True, res[0]


def require_login(f):
    """usage
    @require_login
    def GET(self):
        ..."""
    def decorated(*args, **kwargs):
        if not session.loggedin:
            session.logon_err = "Please Logon"
            return web.seeother("/")
        else:
            session.logon_err = ""
        return f(*args, **kwargs)

    return decorated


class Index:
    def GET(self):
        l = locals()
        del l['self']
        return "It works!!"
        # return render.start(**l)

    def POST(self):
        global session
        params = web.input(username="", password="")
        username = params.username
        password = params.password
        r = auth_user(db, username, password)
        if r[0]:
            session.loggedin = True
            info = r[1]
            session.username = info.firstname + " " + info.lastname
            session.sesid = info.id
            l = locals()
            del l['self']
            return web.seeother("/requests")
        else:
            session.loggedin = False
            session.logon_err = r[1]
        l = locals()
        del l['self']
        return render.logon(**l)


class CreateFacility:
    """Creates and edits an mTrac health facility"""
    # @require_login
    def GET(self):
        params = web.input(
            name="", ftype="", district="",
            uuid="", is_033b='f', dhis2id="", subcounty="", owner="",
            username="", password=""
        )
        username = params.username
        password = params.password
        r = auth_user(db, username, password)
        if not r[0]:
            return "Unauthorized access"

        with db2.transaction() as t:
            res = db2.query(
                "SELECT id FROM healthmodels_healthfacilitytypebase "
                "WHERE lower(name) = $name ",
                {'name': params.ftype.lower()})
            if res:
                type_id = res[0]["id"]
                r = db2.query(
                    "SELECT id FROM healthmodels_healthfacilitybase WHERE code = $id",
                    {'id': params.dhis2id})
                if not r:
                    logging.debug("Creating facility with ID:%s" % params.dhis2id)
                    new = db2.query(
                        "INSERT INTO healthmodels_healthfacilitybase "
                        "(name, code, type_id, district, active, deleted, owner) VALUES "
                        "($name, $dhis2id, $type, $district, $active, $deleted, $owner) RETURNING id",
                        {
                            'name': params.name, 'dhis2id': params.dhis2id,
                            'type': type_id, 'district': params.district,
                            'active': True, 'deleted': False, 'owner': params.owner
                        })
                    t.commit()
                    db2.query(
                        "INSERT INTO healthmodels_fredfacilitydetail "
                        "(uuid_id, h033b) VALUES ($id, $is_033b)",
                        {'id': params.dhis2id, 'is_033b': params.is_033b})
                    if new:
                        facility_id = new[0]["id"]
                        print "FACILITY-ID:", facility_id
                        # db2.query(
                        #     "INSERT INTO healthmodels_healthfacility"
                        #     " (healthfacilitybase_ptr_id) VALUES($id)", {'id': facility_id}
                        # )
                        d = db2.query(
                            "SELECT id FROM locations_location WHERE lower(name) = $district "
                            "AND level = 2", {'district': params.district.lower()})
                        if d:
                            district_id = d[0]["id"]
                            res2 = db2.query(
                                "SELECT id FROM locations_location "
                                "WHERE name ilike $name AND level = 4"
                                " AND get_district(id) = $district",
                                {'name': '%%%s%%' % params.subcounty, 'district': params.district})
                            if res2:
                                # we have a sub county in mTrac
                                subcounty_id = res2[0]["id"]
                                print "WE HAVE SUBCOUNTY:", subcounty_id
                                db2.query(
                                    "INSERT INTO healthmodels_healthfacilitybase_catchment_areas "
                                    "(healthfacilitybase_id, location_id) "
                                    "VALUES ($facility, $loc)",
                                    {'facility': facility_id, 'loc': subcounty_id})
                                logging.debug("Set Facility Location: ID:%s Location:%s" % (params.dhis2id, subcounty_id))
                            else:
                                # make district catchment area
                                print "USING DISTRICT FOR LOCATION:", district_id
                                db2.query(
                                    "INSERT INTO healthmodels_healthfacilitybase_catchment_areas "
                                    "(healthfacilitybase_id, location_id) "
                                    "VALUES ($facility, $loc)",
                                    {'facility': facility_id, 'loc': district_id})
                                logging.debug("Set Facility Location: ID:%s Location:%s" % (params.dhis2id, district_id))
                        logging.debug("Facility with ID:%s sucessfully created." % params.dhis2id)
                    return "Created Facility ID:%s" % params.dhis2id
                else:
                    # facility with passed uuid already exists
                    logging.debug("updating facility with ID:%s" % params.dhis2id)
                    facility_id = r[0]["id"]
                    db2.query(
                        "UPDATE healthmodels_healthfacilitybase SET "
                        "name = $name, code = $dhis2id, type_id = $type, "
                        "district = $district, owner = $owner"
                        " WHERE id = $facility ",
                        {
                            'name': params.name, 'dhis2id': params.dhis2id, 'type': type_id,
                            'district': params.district, 'owner': params.owner, 'facility': facility_id})
                    hmis33b_detail = db2.query(
                        "SELECT id FROM healthmodels_fredfacilitydetail WHERE uuid_id = $id",
                        {'id': params.dhis2id})
                    if hmis33b_detail:
                        db2.query(
                            "UPDATE healthmodels_fredfacilitydetail SET "
                            "h033b = $is_033b WHERE uuid_id = $id",
                            {'is_033b': 't' if params.is_033b else 'f', 'id': params.dhis2id}
                        )
                    else:  # if record doesn't exist
                        db2.query(
                            "INSERT INTO healthmodels_fredfacilitydetail "
                            "(uuid_id, h033b) VALUES ($id, $is_033b)",
                            {'id': params.dhis2id, 'is_033b': params.is_033b})

                    logging.debug("Set h033b for facility with ID:%s to %s" % (params.dhis2id, params.is_033b))
                    d = db2.query(
                        "SELECT id FROM locations_location WHERE lower(name) = $name "
                        "AND level = 2", {'name': params.district.lower()})
                    if d:
                        district_id = d[0]["id"]
                        res2 = db2.query(
                            "SELECT id FROM locations_location WHERE name ilike $name AND level = 4"
                            " AND get_district(id) = $district",
                            {'name': '%%%s%%' % params.subcounty.strip(), 'district': params.district})
                        if res2:
                            # we have a sub county in mTrac
                            subcounty_id = res2[0]["id"]
                            logging.debug(
                                "Sub county:%s set for facility with ID:%s" %
                                (params.subcounty, params.dhis2id))
                            res3 = db2.query(
                                "SELECT id FROM healthmodels_healthfacilitybase_catchment_areas "
                                " WHERE healthfacilitybase_id = $facility AND location_id = $loc",
                                {'facility': facility_id, 'loc': subcounty_id})
                            if not res3:
                                db2.query(
                                    "INSERT INTO healthmodels_healthfacilitybase_catchment_areas "
                                    "(healthfacilitybase_id, location_id) "
                                    "VALUES ($facility, $loc)",
                                    {'facility': facility_id, 'loc': subcounty_id})
                                logging.debug("Set Facility Location: ID:%s Location:%s" % (params.dhis2id, subcounty_id))
                        else:
                            # make district catchment area
                            res3 = db2.query(
                                "SELECT id FROM healthmodels_healthfacilitybase_catchment_areas "
                                "WHERE healthfacilitybase_id = $facility AND location_id = $loc",
                                {'facility': facility_id, 'loc': district_id})
                            if not res3:
                                db2.query(
                                    "INSERT INTO healthmodels_healthfacilitybase_catchment_areas "
                                    "(healthfacilitybase_id, location_id) "
                                    "VALUES ($facility, $loc)",
                                    {'facility': facility_id, 'loc': district_id})
                                logging.debug("Set Facility Location: ID:%s Location:%s" % (params.dhis2id, district_id))
                        logging.debug("Facility with ID:%s sucessfully updated." % params.dhis2id)
                    return "Updated Facility ID:%s" % params.dhis2id
            else:
                return "Unsupported type:%s" % params.ftype

    def POST(self):
        params = web.input()
        l = locals()
        del l['self']
        return "Created!"


class Logout:
    def GET(self):
        session.kill()
        return web.seeother("/")

if __name__ == "__main__":
    app.run()

# makes sure apache wsgi sees our app
application = web.application(urls, globals()).wsgifunc()
