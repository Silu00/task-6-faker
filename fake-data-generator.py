import psycopg2
from psycopg2.extras import execute_values
from faker import Faker

db_config = {
    #Database data
}
schema = "task_6"
target_locations = ['en_US', 'de_DE', 'fr_FR', 'pl_PL', 'it_IT']
items_per_category = 1000
def get_db_connection():
    try:
        connection = psycopg2.connect(**db_config)
        return connection
    except Exception as e:
        print(f"Error connecting to database: {e}")
        exit(1)

def get_physical_attributes(locale=None):
    return {
        'eye_color': ['Blue', 'Brown', 'Green', 'Hazel', 'Grey', 'Amber'],
        'hair_color': ['Black', 'Brown', 'Blond', 'Red', 'Grey', 'White', 'Auburn'],
        'skin_tone': ['Pale', 'Fair', 'Medium', 'Olive', 'Tan', 'Dark', 'Black'],
        'build': ['Slim', 'Average', 'Athletic', 'Muscular', 'Stocky', 'Heavy']
    }

def get_titles(locale):
    if locale == 'pl_PL':
        return ['Pan', 'Inż.', 'Dr', 'Prof.'], ['Pani', 'Dr', 'Prof.']
    elif locale == 'de_DE':
        return ['Herr', 'Dr.', 'Prof.'], ['Frau', 'Dr.', 'Prof.']
    elif locale == 'fr_FR':
        return ['M.', 'Dr'], ['Mme', 'Mlle', 'Dr']
    elif locale == 'it_IT':
        return ['Sig.', 'Dott.'], ['Sig.ra', 'Dott.ssa']
    else:
        return ['Mr.', 'Dr.', 'Prof.'], ['Mrs.', 'Ms.', 'Miss', 'Dr.']

def seed_lookup_data(cur, fake, locale):
    data_map = {
        'fname_male': set(),
        'fname_female': set(),
        'middlename_male': set(),
        'middlename_female': set(),
        'last_name': set(),
        'city': set(),
        'street': set(),
        'email_domain': set(),
        'title_male': set(),
        'title_female': set(),
        'eye_color': set(),
        'hair_color': set(),
        'skin_tone': set(),
        'build': set()
    }
    max_attempts = items_per_category * 3

    for _ in range(max_attempts):
        if len(data_map['fname_male']) < items_per_category:
            data_map['fname_male'].add(fake.first_name_male())
        if len(data_map['fname_female']) < items_per_category:
            data_map['fname_female'].add(fake.first_name_female())
        if len(data_map['last_name']) < items_per_category:
            data_map['last_name'].add(fake.last_name())
        if len(data_map['city']) < items_per_category:
            data_map['city'].add(fake.city())
        if len(data_map['street']) < items_per_category:
            data_map['street'].add(fake.street_name())
        if len(data_map['middlename_male']) < items_per_category:
            data_map['middlename_male'].add(fake.first_name_male())
        if len(data_map['middlename_female']) < items_per_category:
            data_map['middlename_female'].add(fake.first_name_female())

        full_categories = [
            len(data_map[k]) >= items_per_category
            for k in ['fname_male', 'fname_female', 'last_name', 'middlename_male', 'middlename_female']
        ]
        if all(full_categories):
            break

    for _ in range(50):
        data_map['email_domain'].add(fake.free_email_domain())
    data_map['email_domain'].update(['gmail.com', 'yahoo.com', 'outlook.com', 'hotmail.com', 'icloud.com'])

    t_male, t_female = get_titles(locale)
    data_map['title_male'].update(t_male)
    data_map['title_female'].update(t_female)

    physical_attributes = get_physical_attributes(locale)
    data_map['eye_color'].update(physical_attributes['eye_color'])
    data_map['hair_color'].update(physical_attributes['hair_color'])
    data_map['skin_tone'].update(physical_attributes['skin_tone'])
    data_map['build'].update(physical_attributes['build'])

    rows_to_insert = []
    for category, values in data_map.items():
        for value in values:
            rows_to_insert.append((category, locale, value))

    query = f"""
    INSERT INTO {schema}.lookup_data (category, locale, value)
    VALUES %s
    ON CONFLICT (category, locale, value) DO NOTHING 
    """
    execute_values(cur, query, rows_to_insert)
    print(f"[{locale}] Inserted {len(rows_to_insert)} records")

def seed_formatting_rules(cur, locale):
    patterns = []

    if locale == 'en_US':
        patterns.append(('phone_format', locale, '(###) ###-####'))
        patterns.append(('phone_format', locale, '+1-###-###-####'))
        patterns.append(('phone_format', locale, '###-###-####'))

    elif locale == 'de_DE':
        patterns.append(('phone_format', locale, '0## #######'))
        patterns.append(('phone_format', locale, '+49 ## ########'))

    elif locale == 'pl_PL':
        patterns.append(('phone_format', locale, '###-###-###'))
        patterns.append(('phone_format', locale, '+48 ### ### ###'))

    elif locale == 'fr_FR':
        patterns.append(('phone_format', locale, '0# ## ## ## ##'))
        patterns.append(('phone_format', locale, '+33 # ## ## ## ##'))
        patterns.append(('phone_format', locale, '0#.#.#.#.#'))

    elif locale == 'it_IT':
        patterns.append(('phone_format', locale, '3## #######'))
        patterns.append(('phone_format', locale, '+39 3## #######'))
        patterns.append(('phone_format', locale, '0# ########'))
        patterns.append(('phone_format', locale, '0## #######'))

    if locale == 'en_US':
        patterns.append(('postcode_format', locale, '#####'))
        patterns.append(('postcode_format', locale, '#####-####'))
    elif locale == 'pl_PL':
        patterns.append(('postcode_format', locale, '##-###'))
    elif locale in ['de_DE', 'fr_FR', 'it_IT']:
        patterns.append(('postcode_format', locale, '#####'))
    elif locale == 'fr_FR':
        patterns.append(('postcode_format', locale, '#####'))
    elif locale == 'it_IT':
        patterns.append(('postcode_format', locale, '#####'))

    if patterns:
        query = f"""INSERT INTO {schema}.formatting (category, locale, pattern) VALUES %s"""
        execute_values(cur, query, patterns)

def main():
    connection = get_db_connection()
    cur = connection.cursor()
    cur.execute(f"TRUNCATE TABLE {schema}.lookup_data, {schema}.formatting RESTART IDENTITY;")

    Faker.seed(12345)

    for loc in target_locations:
        fake = Faker(loc)
        seed_lookup_data(cur, fake, loc)
        seed_formatting_rules(cur, loc)

    connection.commit()
    cur.close()
    connection.close()
    print("Done.")

if __name__ == '__main__':
    main()
