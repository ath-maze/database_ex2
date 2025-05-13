# ----- CONFIGURE YOUR EDITOR TO USE 4 SPACES PER TAB ----- #
import pymysql
from collections import Counter
import re
import random
import json

# Example usage:
db_config = {
    'host': 'localhost',
    'user': 'root', ##your user name here, usually root
    'password': 'Mypassword1234', ##your password here
    'database': 'Airbnb', ## the name of your database
    # 'charset': 'utf8mb4', #optional
    # 'cursorclass': pymysql.cursors.DictCursor #optional
}

""" By default, pymysql cursors return results as tuples. Each tuple represents a row from the database, 
    and you access the columns by their numerical index (e.g., row[0], row[1]).
    pymysql.cursors.DictCursor changes this behavior. Instead of tuples, it returns results as dictionaries.
"""



def checkIfPropertyExists(location_a, property_type_a):
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()
    
    ### YOUR CODE HERE
    # Πάρε όλα τα property_id με τοποθεσία location_a
    sql1 = """ SELECT property_id FROM property WHERE location = %s """          
    cursor.execute(sql1, (location_a,))
    location_props = set(row[0] for row in cursor.fetchall())
    
    if not location_props:
        cursor.close()
        connection.close()
        return [("no",)]

    # Πάρε το type_id για το property_type_a
    sql2 = """ SELECT type_id FROM propertytype WHERE type_name = %s """ 
    cursor.execute(sql2, (property_type_a,))
    result = cursor.fetchone()

    if not result:
        cursor.close()
        connection.close()
        return [("no",)]

    type_id = result[0]

    # Πάρε όλα τα property_id που έχουν τον συγκεκριμένο type_id
    sql_final = """ SELECT property_id FROM property_has_type WHERE type_id = %s """
    cursor.execute(sql_final, (type_id,))
    type_props = set(row[0] for row in cursor.fetchall())
    
    cursor.close()
    connection.close()

    # Έλεγχος τομής των δύο sets
    if location_props & type_props:
        return [("yes",)]
    else:
        return [("no",)]


def selectTopNhosts(N):
    db = pymysql.connect(**db_config)
    cursor = db.cursor()
    
    ### YOUR CODE HERE
    try:
        # I fetch the property_id of all the properties in location_a
        sql1 = """SELECT type_id FROM propertytype"""          
        cursor.execute(sql1,)
        property_types = set(row[0] for row in cursor.fetchall())

        result = []
        # AS ... is not necessary but for clarity in the code it's used
        for type in property_types:
            sql3 = """SELECT type_name FROM propertytype WHERE type_id = %s"""          
            cursor.execute(sql3, (type,))
            type_name = cursor.fetchone()
            name = type_name[0]

            # I fetch the first N hosts_ids from properties of type, after i group them and order them from most to least frequent
            sql2 = f""" SELECT host_id, COUNT(*) AS num_properties FROM property WHERE property_id = ANY (SELECT property_id FROM property_has_type WHERE type_id = %s) GROUP BY host_id ORDER BY num_properties DESC LIMIT {N} """
            cursor.execute(sql2, (type,))
            for row in cursor.fetchall():
                host_id, num_properties = row
                result.append((name, host_id, num_properties))

        header = [("Property ID", "Host ID", "Property Count")]

    except:
        db.rollback()
        print("")
        return ["no"]


    db.close()
    return header + result
     

def findMatchingProperties(guest_id):
    db = pymysql.connect(**db_config)
    cursor = db.cursor()
    
    ### YOUR CODE HERE
    try:
        # 1. Βρες τα wishlist_id του επισκέπτη
        sql1 = """ SELECT wishlist_id FROM wishlist WHERE guest_id = %s """
        cursor.execute(sql1, (guest_id,))
        wishlist_ids = [row[0] for row in cursor.fetchall()]

        # 2. Παροχές από τις wishlists του χρήστη
        wishlist_property_ids = set()
        for wid in wishlist_ids:
            sql2 = """ SELECT property_id FROM wishlist_has_property WHERE wishlist_id = %s """
            cursor.execute(sql2, (wid,))
            wishlist_property_ids.update([row[0] for row in cursor.fetchall()])

        wishlist_amenities = set()
        for pid in wishlist_property_ids:
            sql3 = """ SELECT amenity_id FROM property_has_amenity WHERE property_id = %s """
            cursor.execute(sql3, (pid,))
            wishlist_amenities.update([row[0] for row in cursor.fetchall()])

        # 3. Κρατήσεις επισκέπτη
        sql4 = """ SELECT property_id FROM booking WHERE guest_id = %s """
        cursor.execute(sql4, (guest_id,))
        booked_properties = set(row[0] for row in cursor.fetchall())

        # 4. Κανόνες από καταλύματα στα οποία έχει κάνει κράτηση
        booked_rules = set()
        for pid in booked_properties:
            sql5 = """ SELECT rule_id FROM property_has_rule WHERE property_id = %s """
            cursor.execute(sql5, (pid,))
            booked_rules.update([row[0] for row in cursor.fetchall()])

        # 5. Οικοδεσπότες από τα καταλύματα που έχει κάνει κράτηση
        booked_hosts = set()
        for pid in booked_properties:
            sql6 = """ SELECT host_id FROM property WHERE property_id = %s """
            cursor.execute(sql6, (pid,))
            result = cursor.fetchone()
            if result:
                booked_hosts.add(result[0])

        # 6. Εξετάζουμε όλα τα καταλύματα
        sql7 = """ SELECT property_id, name, host_id FROM property """
        cursor.execute(sql7, )
        all_properties = cursor.fetchall()

        results = []

        for pid, name, host_id in all_properties:
            if host_id in booked_hosts:
                continue  # Παράβλεψε τον οικοδεσπότη

            # Παροχές του υποψηφίου καταλύματος
            sql8 = """ SELECT amenity_id FROM property_has_amenity WHERE property_id = %s """
            cursor.execute(sql8, (pid,))
            amenities = set(row[0] for row in cursor.fetchall())

            if not amenities & wishlist_amenities:
                continue  # Καμία κοινή παροχή

            # Κανόνες του υποψηφίου καταλύματος
            sql9 = """ SELECT rule_id FROM property_has_rule WHERE property_id = %s """
            cursor.execute(sql9, (pid,))
            rules = set(row[0] for row in cursor.fetchall())

            if not rules & booked_rules:
                continue  # Κανένας κοινός κανόνας

            # Ονόματα παροχών
            amenity_names = []
            for aid in amenities:
                sql10 = """ SELECT amenity_name FROM amenity WHERE amenity_id = %s """
                cursor.execute(sql10, (aid,))
                row = cursor.fetchone()
                if row:
                    amenity_names.append(row[0])

            # Ονόματα κανόνων
            rule_names = []
            for rid in rules:
                sql11 = """ SELECT rule_name FROM houserule WHERE rule_id = %s """
                cursor.execute(sql11, (rid,))
                row = cursor.fetchone()
                if row:
                    rule_names.append(row[0])

            results.append((pid, name, ", ".join(amenity_names) , ", ".join(rule_names)))

        header = [("Property ID", "Name", "Amenities", "Rules")]

    except:
        db.rollback()
        return ["no"]


    db.close()
   
    if (results):
        return header + results
    else:
        return ["no result"]
    

def countWordsForProperties(N, M):
    db = pymysql.connect(**db_config)
    cursor = db.cursor()

    ### YOUR CODE HERE
    N = int(N)
    M = int(M)    
    try:
        # Βήμα 1: Properties με N ή περισσότερους διαφορετικούς επισκέπτες
        sql1 = """  SELECT property_id, COUNT(DISTINCT guest_id) as guest_count
                    FROM booking
                    GROUP BY property_id
                    HAVING guest_count >= %s """
        cursor.execute(sql1, (N,))
        properties_guests = {row[0]: row[1] for row in cursor.fetchall()}

        result = []
        stop_words = set(['the', 'and', 'or', 'but', 'not', 'to', 'at', 'in', 'on', 'a', 'an', 'of', 'for', 'is', 'it', 'this', 'that', 'with', 'as', 'by', 'from', 'are'])

        for property_id, unique_guests in properties_guests.items():
            # Βήμα 2: Έστω και ένας επισκέπτης έχει αφήσει review
            sql2 = "SELECT comment FROM review WHERE property_id = %s"
            cursor.execute(sql2, (property_id,))
            reviews = [row[0] for row in cursor.fetchall()]
            if not reviews:
                continue

            # Βήμα 3: Τουλάχιστον δύο amenities
            sql3 = "SELECT COUNT(*) FROM property_has_amenity WHERE property_id = %s"
            cursor.execute(sql3, (property_id,))
            amenity_count = cursor.fetchone()[0]
            if amenity_count < 2:
                continue

            # Βήμα 4: Δεν υπάρχει σε wishlist
            sql4 = "SELECT * FROM wishlist_has_property WHERE property_id = %s"
            cursor.execute(sql4, (property_id,))
            if cursor.fetchone():
                continue

            # Πληροφορίες για το ακίνητο
            sql5 = "SELECT name, location FROM property WHERE property_id = %s"
            cursor.execute(sql5, (property_id,))
            name, location = cursor.fetchone()

            # Παροχές του ακινήτου
            sql6 = """ SELECT amenity_name FROM amenity WHERE amenity_id IN (SELECT amenity_id FROM property_has_amenity WHERE property_id = %s) """
            cursor.execute(sql6, (property_id,))
            amenities = ', '.join([row[0] for row in cursor.fetchall()])


            # Ανάλυση σχολίων: λέξεις (χωρίς stopwords), μετρήσεις
            words = []
            for comment in reviews:
                cleaned = re.sub(r'[^a-zA-Z ]', '', comment.lower())
                words += [word for word in cleaned.split() if word not in stop_words]

            word_freq = Counter(words)
            top_words = ', '.join([w for w, _ in word_freq.most_common(M)])

            result.append((property_id, name, location, unique_guests, amenities, top_words))

        header = [("Property ID", "Name", "Location", "Unique guests", "Amenities", "Top Words")]
        

    except:
        db.rollback()
        return ["no"]

    db.close()
    return header + result


def findCommonPropertiesAndGuests(guest_id_a, guest_id_b):
    db = pymysql.connect(**db_config)
    cursor = db.cursor()
    
    ### YOUR CODE HERE
    try:
        # I fetch the properties of guest a
        sql1 = """SELECT property_id FROM booking WHERE guest_id = %s"""          
        cursor.execute(sql1, (guest_id_a,))
        properties_a = set(row[0] for row in cursor.fetchall())
        
        # I fetch the properties of guest b
        sql2 = """SELECT property_id FROM booking WHERE guest_id = %s"""          
        cursor.execute(sql2, (guest_id_b,))
        properties_b = set(row[0] for row in cursor.fetchall())

        if not properties_a or not properties_b:
            return ["Guest a or b has no bookings"]

        for propertya in properties_a:
            # I find from properties_a guest c
            sql3 = """SELECT guest_id FROM booking WHERE property_id = %s AND guest_id <> %s """          
            cursor.execute(sql3, (propertya, guest_id_a))
            row_c = cursor.fetchone()
            guest_id_c = row_c[0] if row_c else None


            for propertyb in properties_b:
                # I find from properties_b guest d
                sql4 = """SELECT guest_id FROM booking WHERE property_id = %s AND guest_id <> %s AND guest_id <> %s """          
                cursor.execute(sql4, (propertyb, guest_id_b, guest_id_c))
                row_d = cursor.fetchone()
                guest_id_d = row_d[0] if row_d else None



        if (not guest_id_c or not guest_id_d):
            return ["Guest c or d does not exist"]

        # I find the common properties between c and d
        sql_all = """ (SELECT property_id FROM booking WHERE guest_id = %s)
                   INTERSECT(SELECT property_id FROM booking WHERE guest_id = %s) """
        cursor.execute(sql_all, (guest_id_c, guest_id_d))
        common_properties = set(row[0] for row in cursor.fetchall())


        sql6 = """SELECT name FROM property WHERE property_id = %s"""          
        cursor.execute(sql6, (common_properties,))
        property_names = ', '.join([row[0] for row in cursor.fetchall()])

        header = [("Property Name","Guest C", "Guest D", "Guest A", "Guest B", )]
        result = [(property_names, guest_id_c, guest_id_d, guest_id_a, guest_id_b),]

    except:
        db.rollback()
        print("")
        return ["No match"]

    db.close()
    return header + result


def highValueHost(min_price_booking, min_rating_review, min_avg_price_host, min_avg_rating_host):
    db = pymysql.connect(**db_config)
    cursor = db.cursor()

    try:
        # Βήμα 1
        sql1 = """SELECT host_id, AVG(price) FROM property GROUP BY host_id;"""          
        cursor.execute(sql1)
        avg_price_host = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Βήμα 2
        sql2 = """SELECT host_id, AVG(rating) FROM property GROUP BY host_id;"""          
        cursor.execute(sql2)
        avg_rating_host = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Βήμα 3
        high_value_hosts = {
            host_id for host_id in avg_price_host
            if float(avg_price_host[host_id]) >= float(min_avg_price_host)
            and float(avg_rating_host.get(host_id, 0)) >= float(min_avg_rating_host)
        }
        if not high_value_hosts:
            return [("Amenity", "Frequency")]

        # Βήμα 4
        sql3 = """SELECT guest_id FROM booking WHERE property_id IN (SELECT property_id FROM property WHERE price >= %s);"""
        cursor.execute(sql3, (min_price_booking,))
        guests_price = {row[0] for row in cursor.fetchall()}

        # Βήμα 5
        sql4 = """SELECT guest_id FROM review WHERE rating >= %s;"""
        cursor.execute(sql4, (min_rating_review,))
        guests_rating = {row[0] for row in cursor.fetchall()}

        # Βήμα 6
        high_value_guests = guests_price.intersection(guests_rating)
        if not high_value_guests:
            return [("Amenity", "Frequency")]

        # Βήμα 7
        placeholders = ','.join(['%s'] * len(high_value_hosts))
        sql5 = f"""SELECT property_id FROM property WHERE host_id IN ({placeholders});"""
        cursor.execute(sql5, tuple(high_value_hosts),)
        host_property_ids = {row[0] for row in cursor.fetchall()}
        if not host_property_ids:
            return [("Amenity", "Frequency")]

        # Βήμα 8
        placeholders = ','.join(['%s'] * len(high_value_guests))
        sql6 = f"""SELECT property_id FROM booking WHERE guest_id IN ({placeholders});"""
        cursor.execute(sql6, tuple(high_value_guests),)
        guest_property_ids = {row[0] for row in cursor.fetchall()}
        if not guest_property_ids:
            return [("Amenity", "Frequency")]

        # Βήμα 9
        qualified_properties = host_property_ids.intersection(guest_property_ids)
        if not qualified_properties:
            return [("Amenity", "Frequency")]

        # Βήμα 10
        placeholders = ','.join(['%s'] * len(qualified_properties))
        sql7 = f"""SELECT amenity_id FROM property_has_amenity WHERE property_id IN ({placeholders});"""
        cursor.execute(sql7, tuple(qualified_properties),)
        amenity_ids = [row[0] for row in cursor.fetchall()]
        if not amenity_ids:
            return [("Amenity", "Frequency")]

        # Βήμα 11
        placeholders = ','.join(['%s'] * len(amenity_ids))
        sql8 = f"""SELECT amenity_id, amenity_name FROM amenity WHERE amenity_id IN ({placeholders});"""
        cursor.execute(sql8, tuple(amenity_ids))
        # Δημιουργία ενός λεξικού με την αντιστοίχιση amenity_id -> amenity_name
        amenity_mapping = {row[0]: row[1] for row in cursor.fetchall()}

        # Δημιουργία της λίστας amenities με τα ονόματα να επαναλαμβάνονται ανάλογα με τα amenity_ids
        amenities = [amenity_mapping[amenity_id] for amenity_id in amenity_ids if amenity_id in amenity_mapping]

        if not amenities:
            return [("Amenity", "Frequency")]

        # Συχνότητα
        amenity_freq = Counter(amenities)
        #print("Amenity frequencies:", amenity_freq)
        result = sorted(amenity_freq.items(), key=lambda x: x[1], reverse=True)

        header = [("Amenity", "Frequency")]

    except:
        db.rollback()
        return ["no"]

    db.close()
     
    return header + result


def recommendProperty(guest_id, desired_city, desired_amenities, max_price, min_rating):
    db = pymysql.connect(**db_config)
    cursor = db.cursor()
    
    ### YOUR CODE HERE
    try:
        if isinstance(desired_amenities, str):
            desired_amenities = json.loads(desired_amenities)
            
        max_price_f = float(max_price)
        min_rating_f = float(min_rating)
        # I fetch the property_id of all the properties in location_a
        sql1 = """SELECT property_id FROM property WHERE location = %s AND price <= %s AND rating >= %s"""          
        cursor.execute(sql1, (desired_city, max_price_f, min_rating_f,))
        city_properties = set(row[0] for row in cursor.fetchall())
        
        if not city_properties:
            return ["not found"]
        
        # I fetch the property_id of all the properties in location_a
        sql2 = """SELECT * FROM amenity"""          
        cursor.execute(sql2, )
        all_amenities=cursor.fetchall()
        
        amenities_dict= {row[0]: row[1] for row in all_amenities}
        weighted_amenities = {row[1]: round(random.random(), 2) for row in all_amenities}
        print(weighted_amenities)
        
        
        placeholders = ','.join(['%s'] * len(city_properties))
        sql3 = f"""SELECT property_id, amenity_id FROM property_has_amenity WHERE property_id IN ({placeholders})""" 
        cursor.execute(sql3, tuple(city_properties),)
        prop_amenities = cursor.fetchall()
        
        from collections import defaultdict
        property_amenities = defaultdict(list)
        for pid, aid in prop_amenities:
            property_amenities[pid].append(amenities_dict.get(aid))    
            
        property_scores = []

        for property_id, amenity_list in property_amenities.items():
            score = 0.0
            for amenity in amenity_list:
                factor = desired_amenities.get(amenity, 0)        # σημασία για τον χρήστη
                weight = weighted_amenities.get(amenity, 0)           # βάρος συστήματος
                score += factor * weight
            property_scores.append((property_id, round(score, 2)))  
            #total score με το rating   
        
        # Βρες το ακίνητο με την υψηλότερη βαθμολογία
        if property_scores:
            # Ταξινόμησε για να φέρεις το καλύτερο στην κορυφή
            best_property_id, best_score = max(property_scores, key=lambda x: x[1])
    
            # Πάρε το όνομα του καταλύματος από τη βάση
            cursor.execute("SELECT name FROM property WHERE property_id = %s", (best_property_id,))
            result = cursor.fetchone()
    
            if result:
                best_property_name = result[0]
                return [(best_property_id, best_property_name)]
            else:
                return [("no result", "Name not found")]
        else:
            return [("no result", "No properties matched")]


    except Exception as e:
        db.rollback()
        print("Σφάλμα:", e)
        return ["no"]

    finally:
        db.close()

