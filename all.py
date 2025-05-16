def findCommonPropertiesAndGuests(guest_id_a, guest_id_b):
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()
    results = [("Property Name", "Guest C", "Guest D", "Guest A", "Guest B")]

    try:
        # Βρες καταλύματα του a
        cursor.execute("SELECT DISTINCT property_id FROM booking WHERE guest_id = %s", (guest_id_a,))
        properties_a = [row[0] for row in cursor.fetchall()]

        # Βρες καταλύματα του b
        cursor.execute("SELECT DISTINCT property_id FROM booking WHERE guest_id = %s", (guest_id_b,))
        properties_b = [row[0] for row in cursor.fetchall()]

        for propertya in properties_a:
            # Βρες έναν επισκέπτη c που έμεινε εκεί (όχι ο a)
            sql_c = """SELECT guest_id FROM booking WHERE property_id = %s AND guest_id <> %s"""
            cursor.execute(sql_c, (propertya, guest_id_a))
            row_c = cursor.fetchone()
            guest_id_c = row_c[0] if row_c else None

            if not guest_id_c:
                continue

            for propertyb in properties_b:
                # Βρες έναν επισκέπτη d που έμεινε εκεί (όχι ο b ή c)
                sql_d = """SELECT guest_id FROM booking WHERE property_id = %s AND guest_id NOT IN (%s, %s)"""
                cursor.execute(sql_d, (propertyb, guest_id_b, guest_id_c))
                row_d = cursor.fetchone()
                guest_id_d = row_d[0] if row_d else None

                if not guest_id_d:
                    continue

                # Βρες καταλύματα του c
                cursor.execute("SELECT property_id FROM booking WHERE guest_id = %s", (guest_id_c,))
                props_c = set(row[0] for row in cursor.fetchall())

                # Βρες καταλύματα του d
                cursor.execute("SELECT property_id FROM booking WHERE guest_id = %s", (guest_id_d,))
                props_d = set(row[0] for row in cursor.fetchall())

                # Αν έχουν κοινό κατάλυμα, επιστρέφουμε το αποτέλεσμα
                common = props_c & props_d
                if common:
                    pid = list(common)[0]
                    cursor.execute("SELECT name FROM property WHERE property_id = %s", (pid,))
                    row = cursor.fetchone()
                    prop_name = row[0] if row else f"Property {pid}"

                    results.append((prop_name, guest_id_c, guest_id_d, guest_id_a, guest_id_b))
                    return results  # ✅ επιστροφή μόλις βρούμε ένα έγκυρο ζευγάρι

        return results  # Αν δεν βρεθεί κάτι, επιστρέφει μόνο την κεφαλίδα

    finally:
        cursor.close()
        connection.close()
