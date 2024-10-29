import requests
import json
from datetime import datetime
from requests.exceptions import RequestException
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Tuple, List, Dict
import numpy as np
import nltk
import re
import aiohttp
import asyncio
import nest_asyncio
import random
from tqdm import tqdm
import pandas as pd
import time

### retrieve the list of physical stores:
def getEsselungaStoresList():
    url = "https://www.esselunga.it/services/istituzionale35/all-stores.json"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def getService(services: list, specific_service: str)-> bool:
    codes = []
    for s in services:
        codes.append(s["code"])
    return specific_service in codes 

### return a dict of stores with info about the stores
def getEsselungaStoresInfo(stores: dict) -> dict:
    stores = stores["stores"]
    stores_dict = {}
    for store in stores:
        if store["abbrev"] != None:
            temp = {"address": store["address"],
                    "province": store["province"],
                    "city": store["city"],
                    "CAP": store["zipCode"],
                    "laEsse": store["laEsse"],
                    "latitude": store["latitude"],
                    "longitude": store["longitude"],
                    "clicca_e_vai": getService(store["services"], "CEV"),
                    "consegna_a_domicilio": getService(store["services"], "CON")
            }
            stores_dict[store["abbrev"]] = temp
    return stores_dict

### get the list of streets_id for each post code where there is a store.
### this returns a tuple (dict of streets, list with streets not found)
### the streets not found are usually due to lost/aborted connections
def getStreetsId(stores: dict) -> Tuple[dict, List]:
    headers_template = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
    }
    streets_DB = {}
    postcode_errors = []
    for k in stores.keys():
        store = stores[k]
        postcode = store.get('CAP')
        res = requests.post('https://spesaonline.esselunga.it/commerce/resources/onboarding/postcode/check', json={"postcode":postcode}, headers=headers_template)
        if res.status_code ==200:#.json()["code"] == "SUPPORTED":
            available = requests.post("https://spesaonline.esselunga.it/commerce/resources/onboarding/street/suggestions", json={"postcode":postcode}, headers=headers_template)
            for ava in available.json():
                streets_DB[ava.get("id")] = ava
        else:
            postcode_errors.append(postcode)
    if len(postcode_errors) > 0:
        print("I didn't find {} postcodes.".format(len(postcode_errors)))
    else:
        print("All postcodes found.")
    return streets_DB, postcode_errors

### The following 3 functions are used to retrieve the maximum number of stores/lockers
def getStoresId_list(streets: list, length: int) -> dict:
    storesId = {}
    counter = length
    for k in streets:
        url_drive = "https://spesaonline.esselunga.it/commerce/resources/onboarding/drives/" + str(k) 
        res = requests.get(url_drive)
        for r in res.json():
            storesId[counter] = r
            counter+=1
    return storesId

def fetch_store_data(street_id: str) -> list:
    url_drive = f"https://spesaonline.esselunga.it/commerce/resources/onboarding/drives/{street_id}"
    response = requests.get(url_drive)
    return response.json()

def getStoresId_parallel(streets: dict) -> Tuple[dict, dict, List]:
    storesId = {}
    counter = 0
    errors = []
    # Create a ThreadPoolExecutor to parallelize requests
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Create a dictionary mapping futures to street IDs
        future_to_street = {executor.submit(fetch_store_data, streets[k].get("id")): k for k in streets.keys()}
        # Iterate over the completed futures
        for future in as_completed(future_to_street):
            try:
                data = future.result()
                # Process the result and store in storesId
                for r in data:
                    storesId[counter] = r
                    counter += 1
            except Exception as exc:
                print(f"Street {future_to_street[future]} generated an exception: {exc}")
                errors.append(future_to_street[future])
    print("number of streets missed: {}".format(len(errors))) #len(errors) is < len(new_storesId.keys()) because the list returned by requests.get(url_drive) has len >= 1
    if len(errors) >0:
        new_storesId = getStoresId_list(errors, len(storesId.keys()))
        return storesId, new_storesId, errors
    else:
        return storesId, errors

### function to extract relevant data from the store list
def extractStoresId(stores: dict) -> dict:
    storesId = {}
    for k in stores.keys():
        if isinstance(stores[k], dict):
            name = stores[k].get("name")
            storesId[name] = {"id": stores[k].get("id"), 
                                "streetId": stores[k].get("streetId"),
                                "name": stores[k].get("name"),
                                "code": stores[k].get("code"),
                                "description": stores[k].get("description"),
                                "postCode": stores[k].get("postCode"),
                                "townName": stores[k].get("townName"),
                                "streetName": stores[k].get("streetName"),
                                "houseNumber": stores[k].get("houseNumber"),
                                "latitude": stores[k].get("mapLatitude"),
                                "longitude": stores[k].get("mapLongitude")}
        else:
            next
    return storesId

## filter stores based on city and postcode
def filterDict(storesId: dict, city: str, postcode: str) -> bool:
    return (storesId.get("townName").lower() == city) and (storesId.get("postCode") == postcode)

## find closest store based on distance
def getDistance(lat1: float, long1: float, lat2: float, long2: float) -> float:
    return np.sqrt((lat2 - lat1)**2 + (long2 - long1)**2)

def findClosestStore(store: dict, storesIdList: List[dict]) -> dict:
    closest = storesIdList[0]
    distance = getDistance(store.get("latitude"), store.get("longitude"), storesIdList[0].get("latitude"), storesIdList[0].get("longitude"))
    for i in range(1, len(storesIdList)):
        new_distance = getDistance(store.get("latitude"), store.get("longitude"), storesIdList[i].get("latitude"), storesIdList[i].get("longitude"))
        if new_distance < distance:
            closest = storesIdList[i]
            distance = new_distance
    return closest

## store matcher based on distance
def storeMatcher(storesId: dict, stores: dict) -> dict:
    matched = {}
    for k in stores.keys():
        city = stores[k].get("city").lower()
        postcode = stores[k].get("CAP")
        candidates = [storesId[k] for k in storesId.keys() if filterDict(storesId[k], city, postcode)] 
        if len(candidates) == 1:
            matched[k] = candidates[0].get("name")
        elif len(candidates) == 0:
            matched[k] = ""
        else:
            cand = findClosestStore(stores[k], candidates)
            matched[k] = cand.get("name")
    return matched

## filter stores based on consegna_a_domicilio = true
def filterConsegnaADomicilio(store: dict) -> dict:
    if isinstance(store, dict):
        return store.get("consegna_a_domicilio")
    else:
        False

## filter stores based on clicca_e_vai = true
def filterCliccaEVai(store: dict) -> dict:
    if isinstance(store, dict):
        return store.get("clicca_e_vai")
    else:
        False

## filter stores based on a given custom field:
def filterGivenField(store: dict, field: str, field_value: any) -> dict:
    if isinstance(store, dict):
        if field in store.keys():
            return store.get(field).lower() == field_value
    else:
        print("error in either the key or the value")
        return False

## find closest store based on the address string
def findClosestStore_Lev(store_address: str, streets_address: List[str]) -> int:
    if not streets_address:
        return -1 
    closest = 0
    distance = nltk.edit_distance(store_address, streets_address[closest])
    for i in range(1, len(streets_address)):
        new_distance = nltk.edit_distance(store_address, streets_address[i])
        if new_distance < distance:
            closest = i
            distance = new_distance
    return closest

## store matcher based on address string
def storeInfo(streets: dict, stores: dict) -> dict:
    store_info = {}
    for k in stores.keys():
        try:
            print(k)
            cap = stores[k].get("CAP")
            address = stores[k].get("address").lower()
            substreets = [streets[k] for k in streets.keys() if filterGivenField(streets[k], "postCode", cap)]
            streets_address = [street.get("value").lower().split(" - ")[0] for street in substreets]
            closest = findClosestStore_Lev(address.split(",")[0], streets_address)
            store_info[k] = {"name": k,
                            "street_id": substreets[closest].get("id"), 
                            "address": address,
                            "postCode": cap,
                            "town": stores[k].get("city")}
        except:
            print(f"skipping {k}")
            next
    return store_info

### the following 2 functions are used to map the correspondence between street_id and store_id:
### given a street_id, which store delivers in that street?
def fetch_store_data2(street_id, street_data):
    ssmap = {}
    errors = []
    value = street_data.get("name")
    postCode = street_data.get("postCode")
    town = street_data.get("town")
    base_url = 'https://spesaonline.esselunga.it'
    headers = {
        'Host': 'spesaonline.esselunga.it',
        'X-PAGE-PATH': 'supermercato',
    }
    with requests.Session() as session:
        session.headers.update(headers)
        try:
            response_1 = session.get(f'{base_url}/commerce/nav/drive/store/home', allow_redirects=True)
            store_url = f"{base_url}/commerce/nav/supermercato/visit?streetId={street_id}"
            store_response = session.get(store_url, timeout=20, allow_redirects=False)
            facet_url = f'{base_url}/commerce/resources/search/facet'
            params = {"query": "*", "start": 0, "length": 100, "filters": []}
            response = session.post(facet_url, json=params, timeout=10, allow_redirects=False)
            if response.status_code != 200:
                print(f"Failed to fetch products: {response.status_code}")
                return None, street_id
            try:
                response_json = response.json()
                prod_count = response_json["displayables"]['rowCount']
                print(f"Total products found in store: {prod_count}")
                trolley_url = f"{base_url}/commerce/resources/auth/trolley"
                response4 = session.get(
                    trolley_url,
                    headers={'x-xsrf-token': store_response.cookies.get("XSRF-ECOM-TOKEN"), 'Content-Type': 'application/json'},
                    timeout=10,
                    allow_redirects=False
                )
                if response4.status_code == 200:
                    store_id = response4.json().get("storeId")
                    print("---------------------------------------")
                    print(store_id)
                    print("---------------------------------------")
                    return {store_id: [street_id]}, None
                else:
                    return None, street_id
            except Exception as e:
                print(e)
                return None, street_id
        except Exception as e:
            print(e)
            return None, street_id

def mapStoreidToStreetid(streets):
    ssmap = {}
    errors = []
    # Use ThreadPoolExecutor to parallelize the fetch_store_data function
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_store_data2, str(i), streets[i]): i for i in streets.keys()}
        for future in tqdm(as_completed(futures), total=len(streets)):
            result, error = future.result()
            if result:
                for store_id, street_ids in result.items():
                    if store_id in ssmap:
                        ssmap[store_id].extend(street_ids)
                    else:
                        ssmap[store_id] = street_ids
            if error:
                errors.append(error)
    return ssmap, errors

## function to merge stores (supposed delivery + lockers) into a single dict 
def mergeStores(store_info: dict, storesId: dict) -> dict:
    common_keys= set(store_info.keys()).intersection(storesId.keys())
    if len(common_keys) == 0:
        return {**store_info, **storesId}
    else:
        for k in list(common_keys):
            storesId[k+"_cev"] = storesId.pop(k)
        return {**store_info, **storesId}

## alternative function to find the stores to scrape given the streets to store_id map and the dict of the complete list of stores and lockers
def storesToScrape(ssmap: dict, store_merged: dict) -> dict:
    stores = {}
    lockers = {k:store_merged[k] for k in store_merged.keys() if store_merged[k].get("id") is not None}
    # delivery = {k:store_merged[k] for k in store_merged.keys() if store_merged[k].get("street_id") is not None}
    delivery = [k for k in ssmap.keys() if int(k) < 1000000]
    for d in delivery:
        street_id = random.choice(delivery[d])
        stores[str(d)] = {"street_id": street_id}
    for l in lockers.keys():
        street_id = int(lockers[l].get("streetId"))
        drive_id = int(lockers[l].get("id"))
        stores[l] = {"street_id": street_id, "drive_id": drive_id}
    return stores

#### usage example:
el = getEsselungaStoresList()
el = getEsselungaStoresInfo(el)
start = time.time()
streets = getStreetsId(el)
print("duration: {}".format(time.time() - start))

streets = streets[0]

start = time.time()
stores1, stores2, errors = getStoresId_parallel(streets)
print("duration getStoresId_parallel: {}".format(time.time() - start))
set(stores1.keys()).intersection(stores2.keys()) 

stores = {**stores1, **stores2}
storesId = extractStoresId(stores)

store_info = storeInfo(streets, el)

stores_merged = mergeStores(store_info, storesId)

## this one is rather long:
ssmap = mapStoreidToStreetid(streets)

stores_to_scrape = storesToScrape(ssmap, stores_merged)