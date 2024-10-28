import requests
from fake_useragent import UserAgent
import logging
import pandas as pd
import numpy as np
import re

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def create_session():
    ua = UserAgent()
    session = requests.Session()
    session.headers.update({
        "User-Agent": ua.random,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8"
    })
    logger.info(f"Created session with user agent: {session.headers['User-Agent']}")
    return session

def create_drive_session():
    ua = UserAgent()
    session = requests.Session()
    session.headers.update({
        "User-Agent": ua.random,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "X-PAGE-PATH": "drive"
    })
    logger.info(f"Created session with user agent: {session.headers['User-Agent']}")
    return session

def initial_request(session):
    url = "https://spesaonline.esselunga.it/commerce/nav/onboarding/index"
    return session.get(url)

def initial_request_drive(session):
    headers = {
        'Host': 'spesaonline.esselunga.it',
        'X-PAGE-PATH': 'drive',
    }
    url = 'https://spesaonline.esselunga.it/commerce/nav/drive/store/home'
    return session.get(url, headers=headers)

def visit_supermarket(session, street_id):
    url = "https://spesaonline.esselunga.it/commerce/nav/supermercato/visit?streetId="+str(street_id)
    response = session.get(url, allow_redirects=False)
    if response.status_code == 302:
        redirect_url = response.headers.get('Location')
        logger.info(f"Redirecting to {redirect_url}")
        response = session.get(redirect_url)
    return response

def visit_drive(session, street_id, drive_id):
    url = "https://spesaonline.esselunga.it/commerce/nav/drive/visit?streetId="+str(street_id)+"&driveId"+str(drive_id)
    response = session.get(url, allow_redirects=False)
    if response.status_code == 302:
        redirect_url = response.headers.get('Location')
        logger.info(f"Redirecting to {redirect_url}")
        response = session.get(redirect_url)
    return response


def get_facet_data(session, start: int, length: int = 99):
    url = "https://spesaonline.esselunga.it/commerce/resources/search/facet"
    data = {
        "query": "*",
        "start": start,
        "length": length,
        "filters": []
    }
    return session.post(url, json=data)



### CONSEGNA A CASA ###
def fetch_all_products(store_info: dict, sz: int = 100) -> list:
    street_id = str(store_info.get("street_id"))
    print(f"street_id: {street_id}")
    # value = storeid.get("name")
    # postCode = storeid.get("postCode")
    # town = storeid.get("townName")
    # session = requests.Session()
    # initial_url = 'https://spesaonline.esselunga.it/commerce/nav/drive/store/home'
    # session.get(initial_url)
    # visit_url = "https://spesaonline.esselunga.it/commerce/nav/drive/visit?streetId="+str(street_id)
    # session.get(visit_url)
    # url = "https://spesaonline.esselunga.it/commerce/resources/search/facet"
    session = create_session()
    initial_request(session)
    visit_supermarket(session, street_id)
    start = 0
    prod_count = 20000
    all_products = []
    #visit_supermarket(session, street_id)
    while start < prod_count:
        data = {
        "query": "*",
        "start": start,
        "length": sz,
        "filters": []
        }
        # facet_response = session.post(url, json=data)
        facet_response = get_facet_data(session, 0)
        prod_count = facet_response.json()['displayables']['rowCount']
        if facet_response.status_code != 200:
            print(f"Failed to fetch products: {facet_response.status_code}")
            print(facet_response.text)
            break
        response_cleaned = facet_response.json()["displayables"]["entities"]
        if start == 0:
            print(f"{prod_count} products found")
        all_products.extend(response_cleaned)
        start += sz
    print("Retrieved {} products".format(len(all_products)))
    return all_products


#### CLICCA E VAI ####
def fetch_all_products_CEV(storeid, sz: int = 100) -> list:
    street_id = str(storeid.get("street_id"))
    print(f"street_id: {street_id}")
    # value = storeid.get("name")
    # postCode = storeid.get("postCode")
    # town = storeid.get("townName")
    drive_id = str(storeid.get("drive_id"))
    session = create_drive_session()
    initial_url = 'https://spesaonline.esselunga.it/commerce/nav/drive/store/home'
    session.get(initial_url)
    visit_url = f"https://spesaonline.esselunga.it/commerce/nav/drive/visit?streetId={street_id}&driveId={drive_id}"
    print(visit_url)
    session.get(visit_url)
    url = "https://spesaonline.esselunga.it/commerce/resources/search/facet"
    start = 0
    prod_count = 20000
    all_products = []
    while start < prod_count:
        data = {
        "query": "*",
        "start": start,
        "length": sz,
        "filters": []
        }
        facet_response = session.post(url, json=data)
        prod_count = facet_response.json()['displayables']['rowCount']
        if facet_response.status_code != 200:
            print(f"Failed to fetch products: {facet_response.status_code}")
            print(facet_response.text)
            break
        response_cleaned = facet_response.json()["displayables"]["entities"]
        if start == 0:
            print(f"{prod_count} products found")
        all_products.extend(response_cleaned)
        start += sz
    print("Retrieved {} products".format(len(all_products)))
    return all_products


def extract_dates(text):
    pattern = r"dal (\d{2}/\d{2}/\d{4}) al (\d{2}/\d{2}/\d{4})"
    match = re.search(pattern, text)
    if match:
        return match.group(1), match.group(2)
    else:
        return None


def getProductFields(prod: dict) -> dict:
    pattern = re.compile(r'\s(\d{1,2})\s?[xX]\s?\d{1,4}')
    promo_dict = {}
    if isinstance(prod.get('promo'), list) and len(prod.get('promo')) == 1:
        for i in range(len(prod["txt"])):
            if prod["txt"][i].get("messageType") == "PROMO":
                try:
                    start, end = extract_dates(prod["txt"][i].get("text"))
                    promo_dict["start"] = start
                    promo_dict["end"] = end
                except:
                    print("promo dates not found")
        promo_dict["promoType"] = prod["promo"][0].get("promoType")
        promo_dict["disc_price"] = prod.get("disc_price")
    return {
        "id": prod.get('id'),
        "product_code": prod.get('code'),
        "html": prod.get('htmlDescription'),
        "name": prod.get('name'),
        "n_portions": prod.get("n_portions"),#int(pattern.search(prod.get('description')).group(1)) if pattern.search(prod.get('description')) else None,
        "brand": prod.get('brand'),
        "unit_price": prod.get('label'),
        "price": prod.get('price'),
        "disc_price": prod.get('discountedPrice'),
        "attributes": prod.get('attributes'),
        "txt": prod.get('values'),#[0].get('text') if prod.get('values') else None,
        "variable_weight": prod.get('variableWeight'),
        "oos": prod.get('outOfStock'),
        "prod_type": prod.get('productType'),
        "raee": prod.get('raee'),
        "quantity": prod.get('quantity'),
        "promo": promo_dict,#prod.get('promo'),
        "unit_text": prod.get('unit_text'),
        "unit_value": prod.get('unit_value'),
        "barcode": prod.get('barcode')
    }

def extract_product_info(products: list) -> dict:
    products_extracted = {}
    for prod in products:
        if prod.get('description'):
            fields = getProductFields(prod)
            products_extracted[fields.get("id")] = fields
            children = prod.get("children")
            if isinstance(children, list) and len(children) > 0:
                for child in children:
                    child_field = getProductFields(child)
                    child_field["id"] = child_field['id']
                    products_extracted[child_field.get("id")] = child_field
    return products_extracted


def process_store(store_info: dict, max_retries=2):
    street_id = str(store_info.get("street_id"))
    # value = store_info.get("name")
    # postCode = store_info.get("postCode")
    clicca_e_vai = False
    if store_info.get("drive_id"):
        drive_id = store_info.get("drive_id")
        clicca_e_vai = True
    for attempt in range(max_retries + 1):
        try:
            if clicca_e_vai:
                all_products_data = fetch_all_products_CEV(store_info, sz=99)
                products_extracted = extract_product_info(all_products_data)
                break
            else:
                all_products_data = fetch_all_products(store_info, sz=99)
                products_extracted = extract_product_info(all_products_data)
                break
            # Additional processing of products can go here
        except Exception as e:
            if attempt < max_retries:
                print(f"Attempt {attempt + 1} failed for store {street_id}. Retrying... Error: {str(e)}")
            else:
                print(f"Failed to set store {street_id} after {max_retries + 1} attempts. Skipping this store. {str(e)}")
                return  # Skip this store after all retries fail
            print(f"Error processing store {street_id}: {str(e)}")
    print("for store {} I found {} products".format(street_id, len(products_extracted)))


#### PROD ENRICHMENT ####
def extractURL(attributes_list: list) -> str:
    for i in range(len(attributes_list)):
        if isinstance(attributes_list[i], dict) and attributes_list[i].get("key") == "canonical":
            return attributes_list[i].get("value")

def extractIngredients(informations: list) -> str:
    ingredients = [info for info in informations if info["label"] == "Ingredienti"]
    if len(ingredients) > 0:
        soup = BeautifulSoup(ingredients[0]["value"], 'html.parser')
        ingredient_section = ingredient_section = soup.find('strong', text="Ingredienti").find_parent('p')
        if ingredient_section:
            ingredients_text = ingredient_section.get_text()
            ingredients_text = ingredients_text.replace('Ingredienti', '').strip()
            return ingredients_text
    else:
        print("No ingredients found.")
        return ""

def extractAllergeni(informations: list) -> str:
    ingredients = [info for info in informations if info["label"] == "Ingredienti"]
    if len(ingredients) > 0:
        soup = BeautifulSoup(ingredients[0]["value"], 'html.parser')
        allergeni_section = soup.find('strong', text="Allergeni").find_parent('p')
        if allergeni_section:
            allergeni_text = soup.get_text(separator=" ").replace('Allergeni', '').strip().replace("\n", " ").replace("\t", "")
            allergeni_dict = {
                "Contiene": "",
                "Potrebbe contenere": "",
                "Non contiene": ""
            }
            if "Contiene :" in allergeni_text:
                contiene_part = allergeni_text.split("Contiene :")[1].split("Potrebbe contenere :")[0].strip()
                allergeni_dict["Contiene"] = contiene_part
            if "Potrebbe contenere :" in allergeni_text:
                potrebbe_part = allergeni_text.split("Potrebbe contenere :")[1].split("Non contiene :")[0].strip()
                allergeni_dict["Potrebbe contenere"] = potrebbe_part
            if "Non contiene :" in allergeni_text:
                non_contiene_part = allergeni_text.split("Non contiene :")[1].strip()
                allergeni_dict["Non contiene"] = non_contiene_part
            return allergeni_dict 
    else:
        print("No allergeni found.")
        return ""

def extractNutritionalFacts(informations: list) -> dict:
    vn = [info for info in informations if info["label"] == "Valori nutrizionali"]
    if len(vn) > 0:
        soup = BeautifulSoup(vn[0]["value"], 'html.parser')
        table = soup.find('table')
        nutritional_values = {}
        table = soup.find('table')
        if table:
            for row in table.find_all('tr')[1:]: 
                columns = row.find_all('td')
                if len(columns) >= 2: 
                    nutrient_name = columns[0].get_text(strip=True)
                    nutrient_value = columns[1].get_text(strip=True)
                    nutritional_values[nutrient_name] = nutrient_value
        return nutritional_values
    else:
        return {}

def getProductDetails(product_id):
    """
    !!! N.B.: product_id is the (product) code field in the product dicts: product.get("code") !!!
    """
    url = "https://spesaonline.esselunga.it/commerce/resources/displayable/detail/code/"+str(product_id)
    res = requests.get(url, headers={"accept": "application/json, text/plain, */*", "x-page-path": "supermercato"})
    if res.status_code == 200:
        # url
        url = extractURL(res.json()["seo"].get("attributes"))
        # ingredients
        ingredients = extractIngredients(res.json()["informations"])
        # allergeni
        allergenes = extractAllergeni(res.json()["informations"])
        # nutritional fact
        nutritional_facts = extractNutritionalFacts(res.json()["informations"])
        details = {}
        if len(res.json()["familyChildren"]) == 0:
            details = flatten(res.json()["displayableProduct"])
            details["url"] = url
            details["ingredients"] = ingredients
            details["allergenes"] = allergenes
            details["nutritional_facts"] = nutritional_facts
        else:
            for child in res.json()["familyChildren"]:
                child_dict = flatten(res.json()["displayableProduct"])
                child_dict["id"] = child["productId"]
                child_dict["productId"] = child["productId"]
                child_dict["productCode"] = child["productCode"]
                child_dict["familyAttributes"] = child["familyAttributes"]
                child_dict["sanitizeDescription"] = child["sanitizeDescription"]
                child_dict["description"] = child["sanitizeDescription"]
                child_dict["url"] = url
                child_dict["ingredients"] = ingredients
                child_dict["allergenes"] = allergenes
                child_dict["nutritional_facts"] = nutritional_facts
                details[child["productId"]] = child_dict
    return details
    
#### END PROD ENRICHMENT ####


def get_number_of_products(street_id, drive_id):
    session = requests.Session()
    initial_url = 'https://spesaonline.esselunga.it/commerce/nav/drive/store/home'
    session.get(initial_url)
    visit_url = f"https://spesaonline.esselunga.it/commerce/nav/drive/visit?streetId={street_id}&driveId={drive_id}"
    session.get(visit_url)
    url = "https://spesaonline.esselunga.it/commerce/resources/search/facet"
    data = {
        "query": "*",
        "start": 0,
        "length": 15,
        "filters": []
    }
    response = session.post(url, json=data)
    if response.status_code == 200:
        data = response.json()
        return data["displayables"]["rowCount"]
    else:
        return f"Error: {response.status_code}"

def extractOrderedPrices(data: dict) -> np.array:
    sorted_data = sorted(data.values(), key=lambda x: x["id"])
    prices = [item['price'] for item in sorted_data]
    prices_array = np.array(prices)
    return prices_array

def assignStore(store_id: int, store_info: pd.DataFrame):
    stores = store_info.loc[store_info.store_id == store_id]
    cap = stores.cap.unique().tolist()
    candidates = store_info.loc[
        (store_info.cap.isin(cap)) & 
        ((store_info.description.str.contains("LOCKER")) | 
         (store_info.description.str.contains("CLICCA E VAI")))
    ]
    if stores.shape[0] == 1 and candidates.shape[0] == 1:
        return {store_id: candidates.name2.iloc[0]}
    else:
        Match = ''
        score = 10e6
        possible = ''
        try:
            street_id = str(int(stores.street_id.values[0]))
            session = create_session()
            initial_request(session)
            visit_supermarket(session, street_id)
            facet_response = get_facet_data(session, 0)
            target_prods = facet_response.json()["displayables"]["rowCount"]
            all_products_data = fetch_all_products({"street_id": street_id}, sz=99)
            products_extracted = extract_product_info(all_products_data)
            target_prods_id = set(products_extracted.keys())
            print(target_prods)
            for k in range(candidates.shape[0]):
                drive_id = str(int(candidates.drive_id.values[k]))
                street_id_can = str(int(candidates.street_id.values[k]))
                prods = get_number_of_products(street_id_can, drive_id)
                all_products_data_can = fetch_all_products_CEV({"street_id": street_id_can, "drive_id": drive_id}, sz=99)
                products_extracted_can = extract_product_info(all_products_data_can)
                prods_id = set(products_extracted_can.keys())
                x = str(candidates.name2.values[k])  
                print(prods)
                common_ids = target_prods_id.intersection(prods_id)
                target_p = {k: {"id": products_extracted[k].get("id"), "price": products_extracted[k].get("price")} for k in common_ids}
                can_p = {k: {"id": products_extracted_can[k].get("id"), "price": products_extracted_can[k].get("price")} for k in common_ids}
                target_price = extractOrderedPrices(target_p)
                can_price = extractOrderedPrices(can_p)
                new_score = abs(int(target_prods) - int(prods)) + np.linalg.norm(target_price - can_price) 
                #if prods == target_prods:
                if new_score <= score:
                    score = new_score
                    Match = drive_id
                    #Match.append(x)
        except Exception as e:
            print(f"Error processing store {store_id}: {e}")
            #Match.append("null")    
    return Match

def assigner():
    store_map = pd.read_excel("C:\Python312\map_stores_to_storeid.xlsx")
    assignment = {}
    target_list = [x for x in store_map.store_id.unique().tolist() if int(x) < 1000000]
    for sid in target_list:
        m = assignStore(sid, store_map)
        assignment[sid] = m
    return assignment

def storesToScrape():
    store_map = pd.read_excel("C:/Python312/map_stores_to_storeid.xlsx")
    stores = {}
    lockers = store_map.loc[store_map.drive_id.notnull()]
    delivery = store_map.loc[store_map.drive_id.isnull()]
    for d in delivery.store_id.unique().tolist():
        temp = delivery.loc[delivery.store_id == d]
        if temp.shape[0] > 1:
            street_id = int(temp.street_id.values.ravel()[0])
        else:
            street_id = int(temp.street_id.values.ravel())
        stores[d] = {"street_id": street_id}
    for l in lockers.store_id.unique().tolist():
        temp = lockers.loc[lockers.store_id == l]
        street_id = int(temp.street_id.values.ravel())
        drive_id = int(temp.drive_id.values.ravel())
        stores[l] = {"street_id": street_id, "drive_id": drive_id}
    return stores


def main(store_info):
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(process_store, store_info[k]): k for k in store_info.keys()}
        for future in as_completed(futures):
            store_index = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"Error processing store {store_index}: {str(e)}")



stores = storesToScrape()
start = time.time()
main(stores)
print(f"Time taken: {time.time() - start:.2f} seconds")
