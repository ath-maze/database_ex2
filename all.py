def recommendProperty(guest_id, desired_city, desired_amenities, max_price, min_rating):
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()
    
    ### YOUR CODE HERE
    try:
        if isinstance(desired_amenities, str):
            desired_amenities = json.loads(desired_amenities)
                    
        max_price_f = float(max_price)
        min_rating_f = float(min_rating)
        
        # I fetch the property_id of all the properties in location_a
        sql1 = """SELECT property_id, rating FROM property WHERE location = %s AND price <= %s AND rating >= %s"""          
        cursor.execute(sql1, (desired_city, max_price_f, min_rating_f,))
        city_properties = cursor.fetchall()
        city_properties_dict = {row[0]: float(row[1]) for row in city_properties}
        
        if not city_properties_dict:
            return ["not found"]
        
        # I fetch the property_id of all the properties in location_a
        sql2 = """SELECT * FROM amenity"""          
        cursor.execute(sql2, )
        all_amenities=cursor.fetchall()
        
        amenities_dict= {row[0]: row[1] for row in all_amenities}
        weighted_amenities = {row[1]: round(random.random(), 2) for row in all_amenities}
                
        
        placeholders = ','.join(['%s'] * len(city_properties))
        sql3 = f"""SELECT property_id, amenity_id FROM property_has_amenity WHERE property_id IN ({placeholders})""" 
        cursor.execute(sql3, tuple(city_properties_dict.keys()),)
        prop_amenities = cursor.fetchall()
        
        from collections import defaultdict
        property_amenities = defaultdict(list)
        for pid, aid in prop_amenities:
            amenity_name_1 = amenities_dict.get(aid)
            if amenity_name_1:  # Skip if None
                property_amenities[pid].append(amenity_name_1)
 
              
            
        property_scores = []
        
        for property_id, amenity_list in property_amenities.items():
            amenity_score = 0.0
            for amenity in amenity_list:
                if amenity is None:
                    continue  # skip invalid
                
                factor = float(desired_amenities.get(amenity, 0))
                weight = weighted_amenities.get(amenity, 0)
                amenity_score += factor * weight
        
            
            amenity_score = round(amenity_score, 2)
            rating = city_properties_dict[property_id]
            total_score = round(amenity_score * 0.6 + rating * 0.4, 2)
            property_scores.append((property_id, total_score))
            #total score με το rating   
        
        # Βρες το ακίνητο με την υψηλότερη βαθμολογία
        if property_scores:
            # Ταξινόμησε για να φέρεις το καλύτερο στην κορυφή
            best_property_id, best_score = max(property_scores, key=lambda x: x[1])

            
            # 6. Δημιουργία νέας wishlist
        try:
            wishlist_name = f"Recommended-{uuid.uuid4().hex[:6]}"
            privacy = random.choice(['Private', 'Public'])

            cursor.execute(
                "INSERT INTO wishlist (guest_id, name, privacy) VALUES (%s, %s, %s)",
                (guest_id, wishlist_name, privacy)
            )
            wishlist_id = cursor.lastrowid

            cursor.execute(
                "INSERT INTO wishlist_has_property (wishlist_id, property_id) VALUES (%s, %s)",
                (wishlist_id, best_property_id)
            )

            connection.commit()
            insert_status = "ok"
            
        except Exception as insert_err:
            connection.rollback()
            print("Σφάλμα κατά το insert:", insert_err)
            insert_status = "not ok"

        # 7. Όνομα property
        cursor.execute("SELECT name FROM property WHERE property_id = %s", (best_property_id,))
        result = cursor.fetchone()

        if result:
            best_property_name = result[0]
            return [(best_property_id, best_property_name, insert_status)]
        else:
            return [("no result", "Name not found", insert_status)]


    except:
        connection.rollback()
        return ["no"]

    finally:
        connection.close()