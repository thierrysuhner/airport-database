import psycopg2
import random
import string
from faker import Faker
from datetime import datetime, timedelta

# --- Datenbank-Konfiguration (BITTE ANPASSEN) ---
DB_NAME = "airportmanagementsystem"
DB_USER = "thierrysuhner" 
DB_PASSWORD = "1234"      
DB_HOST = "localhost"
DB_PORT = "5432"
NUM_ROWS = 200

# Faker initialisieren
fake = Faker('de_DE')

# Listen zur Speicherung der generierten Primary Keys (für Foreign Keys)
pks = {
    'Airline': [], 'Terminal': [], 'Stellflaeche': [], 'Passagier': [], 
    'Flughafen': [], 'Kontrolle': [], 'Flug': [], 'Piste': [], 'Gate': [], 
    'Mitarbeiter': [], 'Flaechennutzer': [], 'Fahrzeug': [], 'Parking': [], 
    'Ladenflaeche': [], 'Gepaeck': []
}
iata_codes = set() # Set zur Sicherstellung eindeutiger IATA-Codes

# Hilfsfunktionen zur Datengenerierung

def generate_airport_iata_code(existing_codes):
    """Generiert einen eindeutigen 3-stelligen IATA-Code (A-Z)."""
    while True:
        # Erstellt exakt 3 Grossbuchstaben, um Platzhalter- und Constraint-Probleme zu vermeiden
        code = ''.join(random.choices(string.ascii_uppercase, k=3))
        if code not in existing_codes:
            existing_codes.add(code)
            return code

def get_random_pk(table_name):
    """Gibt einen zufälligen Primary Key aus einer Liste zurück."""
    if not pks[table_name]:
        # Dies sollte nicht passieren, wenn die Reihenfolge korrekt ist
        # Füllt die Lücke, falls eine Tabelle weniger als NUM_ROWS Einträge hat
        return None 
    return random.choice(pks[table_name])

def normalize(text):
    replacements = {
        'ä': 'ae', 'ö': 'oe', 'ü': 'ue',
        'Ä': 'Ae', 'Ö': 'Oe', 'Ü': 'Ue',
        'ß': 'ss'
    }
    for src, tgt in replacements.items():
        text = text.replace(src, tgt)
    return text

# Verbindung und Datenimport

def insert_data():
    conn = None
    try:
        # Verbindung zur Datenbank herstellen
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        cur = conn.cursor()
        print(f"Verbindung zu PostgreSQL hergestellt. Starte Generierung von {NUM_ROWS} Datensätzen...")
    
        # --- Entitäten ohne Fremdschlüssel-Abhängigkeiten ---

        print("\n--- Generiere Entitäten ---")
        
        # Airline
        print("  - Airline...")
        for _ in range(NUM_ROWS):
            iata = generate_airport_iata_code(iata_codes)
            name = normalize(fake.company()).encode('ascii', errors='ignore').decode('ascii')
            land = normalize(fake.country()).encode('ascii', errors='ignore').decode('ascii')
            cur.execute("""
                INSERT INTO Airline (IATACode, Name, Ursprungsland) 
                VALUES (%s, %s, %s) 
                RETURNING IATACode;
            """, (iata, name, land))
            pks['Airline'].append(cur.fetchone()[0])
            
        # Terminal
        print("  - Terminal...")
        for i in range(NUM_ROWS):
            cur.execute("""
                INSERT INTO Terminal (TerminalNummer, StuendlicheKapazitaet) 
                VALUES (%s, %s) 
                RETURNING TerminalID;
            """, (f'T{i+1:02d}', random.randint(500, 5000)))
            pks['Terminal'].append(cur.fetchone()[0])

        # Stellflaeche
        print("  - Stellflaeche...")
        for i in range(NUM_ROWS):
            cur.execute("""
                INSERT INTO Stellflaeche (StellflaechenNummer, StellflaechenKategorie, Flaeche) 
                VALUES (%s, %s, %s) 
                RETURNING StellflaechenID;
            """, (f'S{i+1:03d}', random.choice(['General Aviation', 'Cargo', 'Maintenance']), random.uniform(500, 5000)))
            pks['Stellflaeche'].append(cur.fetchone()[0])

        # Passagier
        print("  - Passagier...")
        for _ in range(NUM_ROWS):
            cur.execute("""
                INSERT INTO Passagier (Geburtsdatum, Vorname, Nachname) 
                VALUES (%s, %s, %s) 
                RETURNING PassagierID;
            """, (fake.date_of_birth(minimum_age=18, maximum_age=80), fake.first_name(), fake.last_name()))
            pks['Passagier'].append(cur.fetchone()[0])

        # Flughafen
        print("  - Flughafen...")
        for _ in range(NUM_ROWS):
            iata = generate_airport_iata_code(iata_codes)
            name = (normalize(fake.city_name()) + ' International Airport').encode('ascii', errors='ignore').decode('ascii')
            land = normalize(fake.country()).encode('ascii', errors='ignore').decode('ascii')
            stadt = normalize(fake.city()).encode('ascii', errors='ignore').decode('ascii')

            cur.execute("""
                INSERT INTO Flughafen (IATACode, Name, Land, Stadt) 
                VALUES (%s, %s, %s, %s) 
                RETURNING IATACode;
            """, (iata, name, land, stadt))
            pks['Flughafen'].append(cur.fetchone()[0])
            
        # Kontrolle
        print("  - Kontrolle...")
        for i in range(NUM_ROWS):
            cur.execute("""
                INSERT INTO Kontrolle (KontrolleNummer, StuendlicheKapazitaet) 
                VALUES (%s, %s) 
                RETURNING KontrolleID;
            """, (f'K{i+1:02d}', random.randint(100, 1000)))
            pks['Kontrolle'].append(cur.fetchone()[0])

        # Flug
        print("  - Flug...")
        for i in range(NUM_ROWS):
            abflug = fake.date_time_this_year(before_now=False, after_now=True)
            ankunft = abflug + timedelta(hours=random.randint(1, 10), minutes=random.randint(0, 59))
            cur.execute("""
                INSERT INTO Flug (FlugNummer, Flugzeugtyp, GeplanteAbflugszeit, GeplanteAnkunftszeit) 
                VALUES (%s, %s, %s, %s) 
                RETURNING FlugID;
            """, (f'FL{i+1:04d}', fake.word(ext_word_list=['Boeing 737', 'Airbus A320', 'Embraer 190']), abflug, ankunft))
            pks['Flug'].append(cur.fetchone()[0])

        # Piste
        print("  - Piste...")
        for i in range(NUM_ROWS):
            cur.execute("""
                INSERT INTO Piste (PistenNummer, Laenge, PistenStatus, Oberflaechenart, ILSKategorie) 
                VALUES (%s, %s, %s, %s, %s) 
                RETURNING PistenID;
            """, (f'P{i+1:02d}', random.uniform(1500, 4500), random.choice(['frei', 'besetzt', 'in Wartung', 'gesperrt']), random.choice(['Asphalt', 'Gras']), random.randint(1, 3)))
            pks['Piste'].append(cur.fetchone()[0])

        # Gate
        print("  - Gate...")
        for _ in range(NUM_ROWS):
            gate_num = random.choice(string.ascii_uppercase) + str(random.randint(10, 99))
            cur.execute("""
                INSERT INTO Gate (GateNummer, GateKategorie, GateStatus) 
                VALUES (%s, %s, %s) 
                RETURNING GateID;
            """, (gate_num, random.choice(['Jetway', 'Bus', 'Fuss']), random.choice(['frei', 'besetzt', 'gesperrt'])))
            pks['Gate'].append(cur.fetchone()[0])
            
        # Mitarbeiter
        print("  - Mitarbeiter...")
        for _ in range(NUM_ROWS):
            cur.execute("""
                INSERT INTO Mitarbeiter (Vorname, Nachname, Geburtsdatum, Strasse, Postleitzahl, Ort) 
                VALUES (%s, %s, %s, %s, %s, %s) 
                RETURNING MitarbeiterID;
            """, (normalize(fake.first_name()), normalize(fake.last_name()), fake.date_of_birth(minimum_age=20, maximum_age=65), fake.street_address(), fake.postcode(), fake.city()))
            pks['Mitarbeiter'].append(cur.fetchone()[0])
            
        # Flaechennutzer
        print("  - Flaechennutzer...")
        for _ in range(NUM_ROWS):
            cur.execute("""
                INSERT INTO Flaechennutzer (Name, FlaechennutzerKategorie, Strasse, Postleitzahl, Ort, KontaktTelefonnummer) 
                VALUES (%s, %s, %s, %s, %s, %s) 
                RETURNING FlaechennutzerID;
            """, (normalize(fake.company()), random.choice(['Airline', 'Retail', 'Service', 'Gastro']), fake.street_address(), fake.postcode(), fake.city(), fake.phone_number()))
            pks['Flaechennutzer'].append(cur.fetchone()[0])
            
        # Fahrzeug
        print("  - Fahrzeug...")
        for _ in range(NUM_ROWS):
            year = str(random.randint(2010, 2023))
            cur.execute("""
                INSERT INTO Fahrzeug (FahrzeugStatus, Herstellungsjahr, Fahrzeugtyp, Nummernschild) 
                VALUES (%s, %s, %s, %s) 
                RETURNING FahrzeugID;
            """, (random.choice(['einsatzfähig', 'in Nutzung', 'in Wartung']), year, random.choice(['Follow-Me-Car', 'Gepäckschlepper', 'Tankfahrzeug']), fake.license_plate()))
            pks['Fahrzeug'].append(cur.fetchone()[0])
            
        # Parking
        print("  - Parking...")
        for i in range(NUM_ROWS):
            cur.execute("""
                INSERT INTO Parking (ParkhausNummer, ParkhausKategorie, AnzahlPlaetze, DistanzZumFlughafen, Auslastung) 
                VALUES (%s, %s, %s, %s, %s) 
                RETURNING ParkhausID;
            """, (f'P{i+1}', random.choice(['Kurzzeit', 'Langzeit', 'Valet', 'Spezial']), random.randint(100, 5000), random.uniform(0.1, 5.0), random.uniform(10.0, 95.0)))
            pks['Parking'].append(cur.fetchone()[0])

        # Ladenflaeche
        print("  - Ladenflaeche...")
        for i in range(NUM_ROWS):
            cur.execute("""
                INSERT INTO Ladenflaeche (Flaeche, LadenflaecheKategorie, LadenflaecheNummer) 
                VALUES (%s, %s, %s) 
                RETURNING LadenflaecheID;
            """, (random.uniform(20, 500), random.choice(['Shop', 'Restaurant', 'Boutique', 'Kiosk']), f'L{i+1:03d}'))
            pks['Ladenflaeche'].append(cur.fetchone()[0])

        # Gepaeck 
        print("  - Gepaeck...")
        for _ in range(NUM_ROWS):
            cat = random.choice(['Handgepaeck klein', 'Handgepaeck gross', 'Aufgabegepaeck', 'Sperrgut'])
            weight = random.randint(0, 32)
            cur.execute("""
                INSERT INTO Gepaeck (GepaeckKategorie, Gewicht) 
                VALUES (%s, %s) 
                RETURNING GepaeckID;
            """, (cat, weight))
            pks['Gepaeck'].append(cur.fetchone()[0])

        # --- Sub-Entitäten und Relationen (FKs) ---
        
        print("\n--- Generiere Sub-Entitäten und Relationen ---")
        
        # SecurityKontrolle
        print("  - SecurityKontrolle...")
        kontrolle_pks = pks['Kontrolle'].copy()
        random.shuffle(kontrolle_pks)
        security_kontrolle_pks = kontrolle_pks[:NUM_ROWS // 3]
        for pk in security_kontrolle_pks:
            cur.execute("""
                INSERT INTO SecurityKontrolle (KontrolleID, eingesetzteScanner) 
                VALUES (%s, %s);
            """, (pk, random.choice(['2D', '3D'])))

        # Zollkontrolle
        print("  - Zollkontrolle...")
        zoll_kontrolle_pks = kontrolle_pks[NUM_ROWS // 3 : 2 * NUM_ROWS // 3]
        for pk in zoll_kontrolle_pks:
            cur.execute("INSERT INTO Zollkontrolle (KontrolleID) VALUES (%s);", (pk,))

        # Passkontrolle
        print("  - Passkontrolle...")
        pass_kontrolle_pks = kontrolle_pks[2 * NUM_ROWS // 3:]
        for pk in pass_kontrolle_pks:
            cur.execute("""
                INSERT INTO Passkontrolle (KontrolleID, automatisiert) 
                VALUES (%s, %s);
            """, (pk, random.choice([True, False])))

        # Reisedokument
        print("  - Reisedokument...")
        for passagier_id in pks['Passagier']:
            num_docs = random.randint(1, 2)
            for i in range(num_docs):
                typ = random.choice(['ID', 'Pass'])
                doc_num = fake.bothify(text='#########??')
                cur.execute("""
                    INSERT INTO Reisedokument (ReisedokumentNummer, PassagierID, Typ) 
                    VALUES (%s, %s, %s) 
                    ON CONFLICT (ReisedokumentNummer) DO NOTHING;
                """, (doc_num + str(i), passagier_id, typ))
                
        # MitarbeiterTelefon
        print("  - MitarbeiterTelefon...")
        for mitarbeiter_id in pks['Mitarbeiter']:
            for _ in range(random.randint(1, 3)): 
                cur.execute("""
                    INSERT INTO MitarbeiterTelefon (Telefonnummer, MitarbeiterID, Typ) 
                    VALUES (%s, %s, %s) 
                    ON CONFLICT (Telefonnummer) DO NOTHING;
                """, (fake.phone_number(), mitarbeiter_id, random.choice(['Mobil', 'Festnetz', 'Arbeit'])))

        # utztAbflugGate & nutztAnkunftGate
        print("  - nutztAbflugGate & nutztAnkunftGate...")
        for flug_id in pks['Flug']:
            gate_abflug = get_random_pk('Gate')
            gate_ankunft = get_random_pk('Gate')
            
            abflug_start = datetime.now() + timedelta(hours=random.randint(1, 24))
            abflug_ende = abflug_start + timedelta(hours=random.randint(1, 3))
            
            ankunft_start = abflug_ende + timedelta(hours=random.randint(1, 10))
            ankunft_ende = ankunft_start + timedelta(hours=random.randint(1, 3))
            
            # nutztAbflugGate
            cur.execute("""
                INSERT INTO nutztAbflugGate (FlugID, GateID, Nutzungsbeginn, Nutzungsende) 
                VALUES (%s, %s, %s, %s) 
                ON CONFLICT (FlugID, GateID) DO NOTHING;
            """, (flug_id, gate_abflug, abflug_start, abflug_ende))
            
            # nutztAnkunftGate
            cur.execute("""
                INSERT INTO nutztAnkunftGate (FlugID, GateID, Nutzungsbeginn, Nutzungsende) 
                VALUES (%s, %s, %s, %s) 
                ON CONFLICT (FlugID, GateID) DO NOTHING;
            """, (flug_id, gate_ankunft, ankunft_start, ankunft_ende))
            
        # durchgeführtIn
        print("  - durchgeführtIn...")
        for kontrolle_id in pks['Kontrolle']:
            for _ in range(random.randint(1, 2)):
                terminal_id = get_random_pk('Terminal')
                cur.execute("""
                    INSERT INTO durchgeführtIn (KontrolleID, TerminalID) 
                    VALUES (%s, %s) 
                    ON CONFLICT (KontrolleID, TerminalID) DO NOTHING;
                """, (kontrolle_id, terminal_id))

        # aufgegebenFuer
        print("  - aufgegebenFuer...")
        for gepaeck_id in pks['Gepaeck']:
            flug_id = get_random_pk('Flug')
            status = random.choice(['Aufgegeben', 'Verladen', 'Angekommen', 'Abgeholt', 'Unbekannt'])
            cur.execute("""
                INSERT INTO aufgegebenFuer (GepaeckID, FlugID, Gepaeckstatus) 
                VALUES (%s, %s, %s) 
                ON CONFLICT (GepaeckID, FlugID) DO NOTHING;
            """, (gepaeck_id, flug_id, status))

        # landetIn & startetVon
        print("  - landetIn & startetVon...")
        for flug_id in pks['Flug']:
            iata_ankunft = get_random_pk('Flughafen')
            iata_abflug = get_random_pk('Flughafen')
            
            # landetIn
            cur.execute("""
                INSERT INTO landetIn (FlugID, IATACode) 
                VALUES (%s, %s) 
                ON CONFLICT (FlugID, IATACode) DO NOTHING;
            """, (flug_id, iata_ankunft.strip()))
            
            # startetVon
            cur.execute("""
                INSERT INTO startetVon (FlugID, IATACode) 
                VALUES (%s, %s) 
                ON CONFLICT (FlugID, IATACode) DO NOTHING;
            """, (flug_id, iata_abflug.strip()))

        # nutztPisteLandung & nutztPisteStart
        print("  - nutztPisteLandung & nutztPisteStart...")
        for flug_id in pks['Flug']:
            piste_landung = get_random_pk('Piste')
            piste_start = get_random_pk('Piste')
            
            zeit_start = datetime.now() + timedelta(hours=random.randint(1, 24))
            zeit_ende = zeit_start + timedelta(minutes=random.randint(15, 60))

            # nutztPisteLandung
            cur.execute("""
                INSERT INTO nutztPisteLandung (FlugID, PistenID, Ankunftsslot) 
                VALUES (%s, %s, %s) 
                ON CONFLICT (FlugID, PistenID) DO NOTHING;
            """, (flug_id, piste_landung, zeit_start))
            
            # nutztPisteStart
            cur.execute("""
                INSERT INTO nutztPisteStart (FlugID, PistenID, Abflugsslot) 
                VALUES (%s, %s, %s) 
                ON CONFLICT (FlugID, PistenID) DO NOTHING;
            """, (flug_id, piste_start, zeit_ende))
            
        # zugeordnetZu
        print("  - zugeordnetZu...")
        for gate_id in pks['Gate']:
            terminal_id = get_random_pk('Terminal')
            cur.execute("""
                INSERT INTO zugeordnetZu (GateID, TerminalID) 
                VALUES (%s, %s) 
                ON CONFLICT (GateID, TerminalID) DO NOTHING;
            """, (gate_id, terminal_id))

        # hatGepaeck
        print("  - hatGepaeck...")
        for passagier_id in pks['Passagier']:
            for _ in range(random.randint(0, 3)):
                gepaeck_id = get_random_pk('Gepaeck')
                if gepaeck_id: # Nur einfügen, wenn PK verfügbar
                    cur.execute("""
                        INSERT INTO hatGepaeck (PassagierID, GepaeckID) 
                        VALUES (%s, %s) 
                        ON CONFLICT (PassagierID, GepaeckID) DO NOTHING;
                    """, (passagier_id, gepaeck_id))

        # bucht
        print("  - bucht...")
        for passagier_id in pks['Passagier']:
            for _ in range(random.randint(1, 2)):
                flug_id = get_random_pk('Flug')
                buchungsklasse = random.choice(string.ascii_uppercase[:4]) # A, B, C oder D
                sitzplatz_nummer = str(random.randint(1, 50)) + random.choice(string.ascii_uppercase[:6])
                cur.execute("""
                    INSERT INTO bucht (TicketNummer, PassagierID, FlugID, Buchungsklasse, BuchungsStatus, SitzplatzNummer) 
                    VALUES (%s, %s, %s, %s, %s, %s) 
                """, (fake.uuid4(), passagier_id, flug_id, buchungsklasse, random.choice(['Gebucht', 'Bestätigt', 'Annuliert']), sitzplatz_nummer))

        # arbeitetAn (MitarbeiterStatus IN)
        print("  - arbeitetAn...")
        for mitarbeiter_id in pks['Mitarbeiter']:
            iata_code = get_random_pk('Flughafen')
            beginn = fake.date_this_decade(before_today=True)
            ende = None if random.random() < 0.8 else fake.date_between(start_date=beginn)
            cur.execute("""
                INSERT INTO arbeitetAn (MitarbeiterID, IATACode, MitarbeiterStatus, Arbeitsbeginn, Arbeitsende, Jobtitel) 
                VALUES (%s, %s, %s, %s, %s, %s) 
                ON CONFLICT (MitarbeiterID, IATACode) DO NOTHING;
            """, (mitarbeiter_id, iata_code.strip(), random.choice(['Anwesend', 'Remote', 'Krank', 'Urlaub', 'Entlassen']), beginn, ende, fake.job()))

        # StellflaecheIstBesetztVon & LadenflaecheIstBesetztVon
        print("  - StellflaecheIstBesetztVon & LadenflaecheIstBesetztVon...")
        for flaeche_nutzer_id in pks['Flaechennutzer']:
            for _ in range(random.randint(1, 3)):
                start = fake.date_time_this_year(before_now=True)
                ende = start + timedelta(days=random.randint(1, 365))
                
                if random.random() < 0.5:
                    stellflaeche_id = get_random_pk('Stellflaeche')
                    if stellflaeche_id:
                        cur.execute("""
                            INSERT INTO StellflaecheIstBesetztVon (StellflaechenID, FlaechennutzerID, Nutzungsbeginn, Nutzungsende) 
                            VALUES (%s, %s, %s, %s) 
                            ON CONFLICT (StellflaechenID, FlaechennutzerID) DO NOTHING;
                        """, (stellflaeche_id, flaeche_nutzer_id, start, ende))
                else:
                    ladenflaeche_id = get_random_pk('Ladenflaeche')
                    if ladenflaeche_id:
                        cur.execute("""
                            INSERT INTO LadenflaecheIstBesetztVon (LadenflaecheID, FlaechennutzerID, Nutzungsbeginn, Nutzungsende) 
                            VALUES (%s, %s, %s, %s) 
                            ON CONFLICT (LadenflaecheID, FlaechennutzerID) DO NOTHING;
                        """, (ladenflaeche_id, flaeche_nutzer_id, start, ende))

        # wirdVermarktetVon
        print("  - wirdVermarktetVon...")
        for flug_id in pks['Flug']:
            airline_code_executing = get_random_pk('Airline')
            cur.execute("""
                INSERT INTO wirdVermarktetVon (IATACode, FlugID, istDurchführendeAirline) 
                VALUES (%s, %s, %s) 
                ON CONFLICT (IATACode, FlugID) DO NOTHING;
            """, (airline_code_executing.strip(), flug_id, True))
            
            if random.random() < 0.2:
                airline_code_marketing = get_random_pk('Airline')
                if airline_code_marketing != airline_code_executing:
                    cur.execute("""
                        INSERT INTO wirdVermarktetVon (IATACode, FlugID, istDurchführendeAirline) 
                        VALUES (%s, %s, %s) 
                        ON CONFLICT (IATACode, FlugID) DO NOTHING;
                    """, (airline_code_marketing.strip(), flug_id, False))
                    
        # betreibtIn & IstTeilVon Relationen
        print("  - IstTeilVon Relationen (betreibtIn, parking, piste, terminal, fahrzeug, ladenflaeche, stellflaeche)...")
        for flughafen_iata in pks['Flughafen']:
            
            # betreibtIn & terminalIstTeilVon
            for _ in range(random.randint(1, 3)): 
                terminal_id = get_random_pk('Terminal')
                if terminal_id:
                    cur.execute("""
                        INSERT INTO betreibtIn (IATACode, TerminalID) 
                        VALUES (%s, %s) 
                        ON CONFLICT (IATACode, TerminalID) DO NOTHING;
                    """, (flughafen_iata.strip(), terminal_id))
                    cur.execute("""
                        INSERT INTO terminalIstTeilVon (TerminalID, IATACode) 
                        VALUES (%s, %s) 
                        ON CONFLICT (TerminalID, IATACode) DO NOTHING;
                    """, (terminal_id, flughafen_iata.strip()))

            # parkingIstTeilVon
            for _ in range(random.randint(1, 5)):
                parking_id = get_random_pk('Parking')
                if parking_id:
                    cur.execute("""
                        INSERT INTO parkingIstTeilVon (ParkhausID, IATACode) 
                        VALUES (%s, %s) 
                        ON CONFLICT (ParkhausID, IATACode) DO NOTHING;
                    """, (parking_id, flughafen_iata.strip()))

            # pisteIstTeilVon
            for _ in range(random.randint(1, 4)):
                pisten_id = get_random_pk('Piste')
                if pisten_id:
                    cur.execute("""
                        INSERT INTO pisteIstTeilVon (PistenID, IATACode) 
                        VALUES (%s, %s) 
                        ON CONFLICT (PistenID, IATACode) DO NOTHING;
                    """, (pisten_id, flughafen_iata.strip()))

            # fahrzeugIstTeilVon
            for _ in range(random.randint(5, 10)):
                fahrzeug_id = get_random_pk('Fahrzeug')
                if fahrzeug_id:
                    cur.execute("""
                        INSERT INTO fahrzeugIstTeilVon (FahrzeugID, IATACode) 
                        VALUES (%s, %s) 
                        ON CONFLICT (FahrzeugID, IATACode) DO NOTHING;
                    """, (fahrzeug_id, flughafen_iata.strip()))
                
            # ladenflaecheIstTeilVon
            for _ in range(random.randint(5, 15)):
                ladenflaeche_id = get_random_pk('Ladenflaeche')
                if ladenflaeche_id:
                    cur.execute("""
                        INSERT INTO ladenflaecheIstTeilVon (LadenflaecheID, IATACode) 
                        VALUES (%s, %s) 
                        ON CONFLICT (LadenflaecheID, IATACode) DO NOTHING;
                    """, (ladenflaeche_id, flughafen_iata.strip()))
                
            # stellflaecheIstTeilVon
            for _ in range(random.randint(5, 15)):
                stellflaeche_id = get_random_pk('Stellflaeche')
                if stellflaeche_id:
                    cur.execute("""
                        INSERT INTO stellflaecheIstTeilVon (StellflaechenID, IATACode) 
                        VALUES (%s, %s) 
                        ON CONFLICT (StellflaechenID, IATACode) DO NOTHING;
                    """, (stellflaeche_id, flughafen_iata.strip()))


        # Transaktion abschliessen
        conn.commit()
        print("\nDaten wurden erfolgreich generiert und in die Datenbank eingefügt.")

    except (Exception, psycopg2.Error) as error:
        print(f"\nFehler beim Einfügen der Daten: {error}")
        if conn:
            conn.rollback()
    finally:
        # Verbindung schliessen
        if conn:
            if cur:
                cur.close()
            conn.close()
            print("PostgreSQL-Verbindung geschlossen.")

if __name__ == "__main__":
    insert_data()