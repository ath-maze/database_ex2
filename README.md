checkIfPropertyExists
https://chatgpt.com/share/6823897a-407c-800a-9ffe-0df0640ea1fd

ΔΙΑΦΟΡΑ:    <!-- sql1 = """ SELECT property_id FROM property WHERE location = %s """          
                 cursor.execute(sql1, (location_a,)) -->
            Τα ερωτήματα της sql είναι γραμμένα με τον παραπάνω τρόπο, για την διευκόλυνση στη γραφή και ανάγνωση του κώδικα. Η διαφοροποίηση αυτή φαίνεται σε όλες τις συναρτήσεις.

            <!-- [("no",)] --> <!-- [("yes",)] -->
            Η συμβολοσειρά που επιστρέφεται κάθε φορά από την συνάρτηση είναι της παραπάνω μορφής για να ακολουθεί τις προδιαγραφές της εκφώνησης




https://chatgpt.com/share/6823897a-407c-800a-9ffe-0df0640ea1fd
και
https://chatgpt.com/share/682397ae-bc68-800a-a60c-75209452dae7

ΔΙΑΦΟΡΑ:    Επιλέγω τα διαφορετικά type_ids από το propertytype και όχι το property_has_type.
            Για να τρέχει ο κώδικας το ερώτημα sql πρέπει να γραφτεί με τη μορφή:
                        <!-- sql3 = f""" SELECT host_id ... DESC LIMIT {N} """ -->
            δηλαδή με f-string, αφού το Ν είναι αριθμός που παίρνω σαν όρισμα και όχι τιμή από τη βάση δεδομένων.

        