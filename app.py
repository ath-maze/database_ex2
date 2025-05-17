# ----- CONFIGURE YOUR EDITOR TO USE 4 SPACES PER TAB ----- #
import pymysql
from collections import Counter
from collections import defaultdict
import re
import random
import json
import uuid
import datetime

# Example usage:
db_config = {
    'host': 'localhost',
    'user': 'root', ##your user name here, usually root
    'password': 'Mypassword1234', ##your password here
    'database': 'Airbnb', ## the name of your database
    # 'charset': 'utf8mb4', #optional
    # 'cursorclass': pymysql.cursors.DictCursor #optional
}

STOP_WORDS = {
    'the','and','or','not','to','at','a','an','in','on','for','of','with','is','it','this','that',
    'these','those','be','been','am','are','was','were','by','as','from','but','if','into','out',
    'about','against','over','under','again','further','then','once','here','there','when','where',
    'why','how','all','any','both','each','few','more','most','other','some','such','no','nor',
    'only','own','same','so','than','too','very','can','will','just','don','should','now'
}

""" By default, pymysql cursors return results as tuples. Each tuple represents a row from the database, 
    and you access the columns by their numerical index (e.g., row[0], row[1]).
    pymysql.cursors.DictCursor changes this behavior. Instead of tuples, it returns results as dictionaries.
"""



def checkIfPropertyExists(location_a, property_type_a):
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()
    
    ### YOUR CODE HERE
    try:
        # Παίρνω όλα τα property_id με τοποθεσία location_a
        sql1 = """ SELECT property_id FROM property WHERE location = %s """          
        cursor.execute(sql1, (location_a,))
        location_props = set(row[0] for row in cursor.fetchall())
        
        if not location_props:
            cursor.close()
            connection.close()
            return [("no",)]

        # Παίρνω το type_id για το property_type_a
        sql2 = """ SELECT type_id FROM propertytype WHERE type_name = %s """ 
        cursor.execute(sql2, (property_type_a,))
        result = cursor.fetchone()

        if not result:
            cursor.close()
            connection.close()
            return [("no",)]

        type_id = result[0]

        # Παίρνω όλα τα property_id που έχουν τον συγκεκριμένο type_id
        sql_final = """ SELECT property_id FROM property_has_type WHERE type_id = %s """
        cursor.execute(sql_final, (type_id,))
        type_props = set(row[0] for row in cursor.fetchall())

    finally:
        cursor.close()
        connection.close()        

    # Έλεγχος τομής των δύο sets
    if location_props & type_props:
        return [("yes",)]
    else:
        return [("no",)]


def selectTopNhosts(N):
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()
    
    ### YOUR CODE HERE
    try: 
        # Παίρνω όλα τα type_id από τον πίνακα property_has_type
        sql1 = """ SELECT type_id FROM propertytype """          
        cursor.execute(sql1,)
        all_type_ids = set(row[0] for row in cursor.fetchall())

        result = []
        for type in all_type_ids:
            # Παίρνω το όνομα κάθε τύπου (type_id -> type_name)
            sql2 = """ SELECT type_name FROM propertytype WHERE type_id = %s """          
            cursor.execute(sql2, (type,))
            type_name = cursor.fetchone()
            name = type_name[0]

            if not name:
                continue

            # Ερώτημα για να πάρω τον αριθμό των properties για κάθε host για το συγκεκριμένο type_id
            sql3 = f""" SELECT host_id, COUNT(*) AS num_properties FROM property WHERE property_id = ANY (SELECT property_id FROM property_has_type WHERE type_id = %s) GROUP BY host_id ORDER BY num_properties DESC LIMIT {N} """
            cursor.execute(sql3, (type,))
            top_hosts = cursor.fetchall()

            # Προσθέτω τα αποτελέσματα
            for host_id, count in top_hosts:
                result.append((name, host_id, count))

        header = [("Property Type", "Host ID", "Property Count")]

    finally:
        cursor.close()
        connection.close()

    return header + result
     

def findMatchingProperties(guest_id):
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()
    
    ### YOUR CODE HERE
    try:
        # 1. Όλα τα properties που υπάρχουν σε ΟΛΕΣ τις wishlists του επισκέπτη
        sql1 = """ SELECT wishlist_id FROM wishlist WHERE guest_id = %s """
        cursor.execute(sql1, (guest_id,))
        wishlist_ids = [row[0] for row in cursor.fetchall()]

        wishlist_property_ids = set()
        for wid in wishlist_ids:
            sql2 = """ SELECT property_id FROM wishlist_has_property WHERE wishlist_id = %s """
            cursor.execute(sql2, (wid,))
            wishlist_property_ids.update([row[0] for row in cursor.fetchall()])

        # 2. Παροχές των wishlist‑properties
        wishlist_amenity_ids = set()
        for pid in wishlist_property_ids:
            sql3 = """ SELECT amenity_id FROM property_has_amenity WHERE property_id = %s """
            cursor.execute(sql3, (pid,))
            wishlist_amenity_ids.update([row[0] for row in cursor.fetchall()])

        # 3. Properties που έχει κάνει κράτηση ο επισκέπτης
        sql4 = """ SELECT property_id FROM booking WHERE guest_id = %s """
        cursor.execute(sql4, (guest_id,))
        booked_property_ids = set(row[0] for row in cursor.fetchall())

        # 4. Κανόνες των booked‑properties + host_ids προς αποκλεισμό
        booked_rule_ids = set()
        excluded_host_ids = set()
        for pid in booked_property_ids:
            # rules
            sql5 = """ SELECT rule_id FROM property_has_rule WHERE property_id = %s """
            cursor.execute(sql5, (pid,))
            booked_rule_ids.update([row[0] for row in cursor.fetchall()])

            # hosts
            sql6 = """ SELECT host_id FROM property WHERE property_id = %s """
            cursor.execute(sql6, (pid,))
            host_row = cursor.fetchone()
            if host_row:
                excluded_host_ids.add(host_row[0])

        # 5. Ανάκτηση όλων των υποψήφιων properties
        sql7 = """ SELECT property_id, name, host_id FROM property """
        cursor.execute(sql7, )
        all_properties = cursor.fetchall()

        results = []

        for pid, name, host_id in all_properties:
            if host_id in excluded_host_ids:
                continue

            # Παροχές του υποψηφίου property
            sql8 = """ SELECT amenity_id FROM property_has_amenity WHERE property_id = %s """
            cursor.execute(sql8, (pid,))
            amenity_ids = set(row[0] for row in cursor.fetchall())

            if not amenity_ids & wishlist_amenity_ids:
                continue

            # Κανόνες του υποψηφίου property
            sql9 = """ SELECT rule_id FROM property_has_rule WHERE property_id = %s """
            cursor.execute(sql9, (pid,))
            rule_ids = set(row[0] for row in cursor.fetchall())

            if not (rule_ids & booked_rule_ids):
                continue

            # amenity_ids και τα ονόματα τους
            amenity_names = []
            for aid in amenity_ids:
                sql10 = """ SELECT amenity_name FROM amenity WHERE amenity_id = %s """
                cursor.execute(sql10, (aid,))
                result = cursor.fetchone()
                if result:
                    amenity_names.append(result[0])
            amenities = ", ".join(amenity_names)

            # rule_ids και τα ονόματα των κανόνων
            rule_names = []
            for rid in rule_ids:
                sql11 = """ SELECT rule_name FROM houserule WHERE rule_id = %s """
                cursor.execute(sql11, (rid,))
                result = cursor.fetchone()
                if result:
                    rule_names.append(result[0])

            rules = ", ".join(rule_names)
            results.append((pid, name, amenities, rules))

        header = [("Property ID", "Name", "Amenities", "Rules")]

    finally:
        cursor.close()
        connection.close()

    return header + results
    

def countWordsForProperties(N, M):
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()
    ### YOUR CODE HERE
    N = int(N)
    M = int(M)    
    try:
        results = []
        
        # Βρες τα property_id που έχουν τουλάχιστον N διαφορετικούς επισκέπτες
        sql1 = """ SELECT property_id FROM booking GROUP BY property_id HAVING COUNT(DISTINCT guest_id) >= %s """
        cursor.execute(sql1, (N,))
        valid_ids = [row[0] for row in cursor.fetchall()]

        for pid in valid_ids:
            # Βρες πόσους μοναδικούς επισκέπτες έχει (ξανά – ή το κρατάμε σε dict για αποδοτικότητα)
            sql2 = """ SELECT COUNT(DISTINCT guest_id) FROM booking WHERE property_id = %s """
            cursor.execute(sql2, (pid,))
            unique_guests = cursor.fetchone()[0]

            # Πάρε όνομα και τοποθεσία
            sql3 = """ SELECT name, location FROM property WHERE property_id = %s """
            cursor.execute(sql3, (pid,))
            result = cursor.fetchone()
            if not result:
                continue
            name, location = result

            # Έχει τουλάχιστον 1 review;
            sql4 = """ SELECT COUNT(*) FROM review WHERE property_id = %s """
            cursor.execute(sql4, (pid,))
            if cursor.fetchone()[0] == 0:
                continue

            # Έχει τουλάχιστον 2 amenities;
            sql5 = """ SELECT a.amenity_name FROM amenity a, property_has_amenity pha WHERE a.amenity_id = pha.amenity_id AND pha.property_id = %s """
            cursor.execute(sql5, (pid,))
            amenities = [row[0] for row in cursor.fetchall()]
            if len(amenities) < 2:
                continue

            # Δεν είναι σε wishlist;
            sql6 = """ SELECT COUNT(*) FROM wishlist_has_property WHERE property_id = %s """
            cursor.execute(sql6, (pid,))
            if cursor.fetchone()[0] > 0:
                continue

            # Πάρε όλα τα σχόλια
            sql7 = """ SELECT comment FROM review WHERE property_id = %s """
            cursor.execute(sql7, (pid,))
            comments = " ".join(row[0] or "" for row in cursor.fetchall())

            words = re.findall(r"[A-Za-z']+", comments.lower())
            filtered = [w for w in words if w not in STOP_WORDS]
            top_words = [w for w, _ in Counter(filtered).most_common(M)]

            # Αποθήκευση
            results.append((pid, name, location, unique_guests, ", ".join(amenities), ", ".join(top_words)))

        results.append(("Total properties", len(results) - 1))

        header = [("Property ID", "Name", "Location", "Unique guests", "Amenities", f"Top {M} Words")]
    finally:
        cursor.close()
        connection.close()

    return header + results


def findCommonPropertiesAndGuests(guest_id_a, guest_id_b):
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()
    
    ### YOUR CODE HERE
    try:
        # 1. Καταλύματα όπου έμεινε ο a
        sql1 = """SELECT property_id FROM booking WHERE guest_id = %s"""          
        cursor.execute(sql1, (guest_id_a,))
        properties_a = set(row[0] for row in cursor.fetchall())
        
        # 2. Καταλύματα όπου έμεινε ο b
        sql2 = """SELECT property_id FROM booking WHERE guest_id = %s"""          
        cursor.execute(sql2, (guest_id_b,))
        properties_b = set(row[0] for row in cursor.fetchall())

        if not properties_a or not properties_b:
            return ["Guest a or b has no bookings"]

        # 3. Βρες τους επισκέπτες που έχουν μείνει στα ίδια καταλύματα με τον a
        for propertya in properties_a:
            # Βρες έναν επισκέπτη c που έμεινε εκεί (όχι ο a)
            sql3 = """SELECT guest_id FROM booking WHERE property_id = %s AND guest_id <> %s """          
            cursor.execute(sql3, (propertya, guest_id_a))
            row_c = cursor.fetchone()
            guest_id_c = row_c[0] if row_c else None

            if not guest_id_c:
                continue

            for propertyb in properties_b:
                # Βρες έναν επισκέπτη d που έμεινε εκεί (όχι ο b ή c)
                sql4 = """SELECT guest_id FROM booking WHERE property_id = %s AND guest_id <> %s AND guest_id <> %s """          
                cursor.execute(sql4, (propertyb, guest_id_b, guest_id_c))
                row_d = cursor.fetchone()
                guest_id_d = row_d[0] if row_d else None

                if not guest_id_d:
                    continue

                # Βρίσκω τα κοινά καταλύματα μεταξύ των c και d
                sql5 = """ SELECT property_id FROM booking WHERE guest_id = %s """
                cursor.execute(sql5, (guest_id_c,))
                properties_c = set(row[0] for row in cursor.fetchall())

                sql6 = """ SELECT property_id FROM booking WHERE guest_id = %s """
                cursor.execute(sql6, (guest_id_d,))
                properties_d = set(row[0] for row in cursor.fetchall())

                common_properties = properties_c & properties_d

                if common_properties:
                    pid = list(common_properties)[0]
                    sql7 = """ SELECT name FROM property WHERE property_id = %s """
                    cursor.execute(sql7, (pid,))
                    property_names = ', '.join([row[0] for row in cursor.fetchall()])


                header = [("Property Name","Guest C", "Guest D", "Guest A", "Guest B", )]
                result = [(property_names, guest_id_c, guest_id_d, guest_id_a, guest_id_b),]

    finally:
        cursor.close()
        connection.close()

    return header + result


def highValueHost(min_price_booking, min_rating_review, min_avg_price_host, min_avg_rating_host):
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()

    try:
        header = [("Amenity", "Frequency")]
        # Βήμα 1
        sql1 = """ SELECT host_id, AVG(price) FROM property GROUP BY host_id """          
        cursor.execute(sql1)
        avg_price_host = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Βήμα 2
        sql2 = """ SELECT host_id, AVG(rating) FROM property GROUP BY host_id """          
        cursor.execute(sql2)
        avg_rating_host = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Βήμα 3
        high_value_hosts = {
            host_id for host_id in avg_price_host
            if float(avg_price_host[host_id]) >= float(min_avg_price_host)
            and float(avg_rating_host.get(host_id, 0)) >= float(min_avg_rating_host)
        }
        if not high_value_hosts:
            return header

        # Βήμα 4
        sql3 = """ SELECT guest_id FROM booking WHERE property_id IN (SELECT property_id FROM property WHERE price >= %s) """
        cursor.execute(sql3, (min_price_booking,))
        guests_price = {row[0] for row in cursor.fetchall()}

        # Βήμα 5
        sql4 = """ SELECT guest_id FROM review WHERE rating >= %s """
        cursor.execute(sql4, (min_rating_review,))
        guests_rating = {row[0] for row in cursor.fetchall()}

        # Βήμα 6
        high_value_guests = guests_price.intersection(guests_rating)
        if not high_value_guests:
            return header

        # Βήμα 7
        placeholders = ','.join(['%s'] * len(high_value_hosts))
        sql5 = f""" SELECT property_id FROM property WHERE host_id IN ({placeholders}) """
        cursor.execute(sql5, tuple(high_value_hosts),)
        host_property_ids = {row[0] for row in cursor.fetchall()}
        if not host_property_ids:
            return header

        # Βήμα 8
        placeholders = ','.join(['%s'] * len(high_value_guests))
        sql6 = f""" SELECT property_id FROM booking WHERE guest_id IN ({placeholders}) """
        cursor.execute(sql6, tuple(high_value_guests),)
        guest_property_ids = {row[0] for row in cursor.fetchall()}
        if not guest_property_ids:
            return header

        # Βήμα 9
        qualified_properties = host_property_ids.intersection(guest_property_ids)
        if not qualified_properties:
            return header
        
        # Βήμα 10
        placeholders = ','.join(['%s'] * len(qualified_properties))
        sql7 = f""" SELECT amenity_id FROM property_has_amenity WHERE property_id IN ({placeholders}) """
        cursor.execute(sql7, tuple(qualified_properties),)
        amenity_ids = [row[0] for row in cursor.fetchall()]
        if not amenity_ids:
            return header

        # Βήμα 11
        placeholders = ','.join(['%s'] * len(amenity_ids))
        sql8 = f""" SELECT amenity_id, amenity_name FROM amenity WHERE amenity_id IN ({placeholders}) """
        cursor.execute(sql8, tuple(amenity_ids))
        # Δημιουργία ενός λεξικού με την αντιστοίχιση amenity_id -> amenity_name
        amenity_mapping = {row[0]: row[1] for row in cursor.fetchall()}

        # Δημιουργία της λίστας amenities με τα ονόματα να επαναλαμβάνονται ανάλογα με τα amenity_ids
        amenities = [amenity_mapping[amenity_id] for amenity_id in amenity_ids if amenity_id in amenity_mapping]

        if not amenities:
            return header

        # Συχνότητα
        amenity_freq = Counter(amenities)
        result = sorted(amenity_freq.items(), key=lambda x: x[1], reverse=True)

    except:
        connection.rollback()
        return ["no"]

    connection.close()
     
    return header + result


def recommendProperty(guest_id, desired_city, desired_amenities, max_price, min_rating):
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()

    try:
        if isinstance(desired_amenities, str):
            desired_amenities = json.loads(desired_amenities)

        
        # Βήμα 1: Βρες όλα τα properties που πληρούν τα βασικά κριτήρια
        sql1 = """ SELECT property_id, name, rating, price FROM property WHERE location = %s AND price <= %s AND rating >= %s """
        cursor.execute(sql1, (desired_city, max_price, min_rating))
        properties = cursor.fetchall()

        if not properties:
            return [("No matching properties found.",)]

        # Βήμα 2: Αντιστοίχισε όνομα παροχής με id
        amenity_weights = desired_amenities.copy()
        amenity_ids = {}
        for amenity_name in amenity_weights.keys():
            sql2 = """ SELECT amenity_id FROM amenity WHERE amenity_name = %s """
            cursor.execute(sql2, (amenity_name,))
            row = cursor.fetchone()
            if row:
                amenity_ids[row[0]] = amenity_weights[amenity_name]

        if not amenity_ids:
            return [("No valid amenities found.",)]

        # Βήμα 3: Υπολογισμός σκορ για κάθε κατάλυμα
        best_property = None
        best_score = -1

        for prop in properties:
            prop_id, prop_name, prop_rating, prop_price = prop

            # Βρες τα amenities του καταλύματος
            sql3 = """ SELECT amenity_id FROM property_has_amenity WHERE property_id = %s """
            cursor.execute(sql3, (prop_id,))
            prop_amenities = [row[0] for row in cursor.fetchall()]

            # Υπολογισμός amenity_score (σταθμισμένο άθροισμα)
            amenity_score = sum(amenity_ids[aid] for aid in prop_amenities if aid in amenity_ids)

            # Συνδυασμένο σκορ (γραμμικός συνδυασμός)
            combined_score = (amenity_score * 0.6) + (float(prop_rating) * 0.4)

            if combined_score > best_score:
                best_score = combined_score
                best_property = (prop_id, prop_name)

        if best_property is None:
            return [("No suitable property found.",)]

        # Βήμα 4: Δημιουργία νέας wishlist
        try:
            wishlist_name = f"Recommended_{uuid.uuid4().hex}"
            privacy = random.choice(['Public', 'Private'])

            cursor.execute(
                "INSERT INTO wishlist (guest_id, name, privacy) VALUES (%s, %s, %s)",
                (guest_id, wishlist_name, privacy)
            )
            wishlist_id = cursor.lastrowid or random.randint(100000, 999999)

            cursor.execute(
                "INSERT INTO wishlist_has_property (wishlist_id, property_id) VALUES (%s, %s)",
                (wishlist_id, best_property[0])
            )

            connection.commit()
            insert_status = "ok"
            

        except Exception as insert_error:
            connection.rollback()
            insert_status = "not ok"
        
        header  = [("Best Property id", "Best Property Name", "Insertion Status"),]
        results = [(best_property[0], best_property[1], insert_status),]

    except Exception as e:
        connection.rollback()
        return [("Error",), (str(e),)]

    finally:
        cursor.close()
        connection.close()

    return header + results