import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_listings_drop = "DROP TABLE IF EXISTS staging_listings;"
staging_reviews_drop = "DROP TABLE IF EXISTS staging_reviews;"
staging_restaurants_drop = "DROP TABLE IF EXISTS staging_restaurants;"
staging_attractions_drop = "DROP TABLE IF EXISTS staging_attractions;"

airbnb_listings_drop = "DROP TABLE IF EXISTS airbnb_listings;"
airbnb_hosts_drop = "DROP TABLE IF EXISTS airbnb_hosts;"
airbnb_reviews_drop = "DROP TABLE IF EXISTS airbnb_reviews;"
cities_drop = "DROP TABLE IF EXISTS cities;"
restaurants_drop = "DROP TABLE IF EXISTS restaurants;"
attractions_drop = "DROP TABLE IF EXISTS attractions;"

# CREATE TABLES

## staging tables

staging_listings_create= ("""
CREATE TEMPORARY TABLE IF NOT EXISTS staging_listings (
    id int,
    listing_url varchar(100),
    scrape_id bigint,
    last_scraped varchar(10),
    name varchar(200),
    summary varchar(1000),
    space varchar(1000),
    description varchar(1000),
    experiences_offered varchar(4),
    neighborhood_overview varchar(1000),
    notes varchar(1000),
    transit varchar(1000),
    access varchar(1000),
    interaction varchar(1000),
    house_rules varchar(1000),
    thumbnail_url varchar(0),
    medium_url varchar(0),
    picture_url varchar(146),
    xl_picture_url varchar(0),
    host_id int,
    host_url varchar(43),
    host_name varchar(35),
    host_since varchar(10),
    host_location varchar(57),
    host_about varchar(8920),
    host_response_time varchar(18),
    host_response_rate varchar(4),
    host_acceptance_rate varchar(4),
    host_is_superhost varchar(1),
    host_thumbnail_url varchar(106),
    host_picture_url varchar(109),
    host_neighbourhood varchar(35),
    host_listings_count varchar(3),
    host_total_listings_count varchar(3),
    host_verifications varchar(158),
    host_has_profile_pic varchar(1),
    host_identity_verified varchar(1),
    street varchar(59),
    neighbourhood varchar(35),
    neighbourhood_cleansed varchar(38),
    neighbourhood_group_cleansed varchar(0),
    city varchar(28),
    state varchar(34),
    zipcode varchar(17),
    market varchar(21),
    smart_location varchar(41),
    country_code varchar(2),
    country varchar(11),
    latitude decimal,
    longitude decimal,
    is_location_exact varchar(1),
    property_type varchar(22),
    room_type varchar(15),
    accommodates smallint,
    bathrooms varchar(4),
    bedrooms varchar(2),
    beds varchar(2),
    bed_type varchar(13),
    amenities varchar(1077),
    square_feet varchar(4),
    price varchar(9),
    weekly_price varchar(9),
    monthly_price varchar(10),
    security_deposit varchar(9),
    cleaning_fee varchar(7),
    guests_included smallint,
    extra_people varchar(7),
    minimum_nights smallint,
    maximum_nights smallint,
    minimum_minimum_nights smallint,
    maximum_minimum_nights smallint,
    minimum_maximum_nights bigint,
    maximum_maximum_nights bigint,
    minimum_nights_avg_ntm decimal,
    maximum_nights_avg_ntm decimal,
    calendar_updated varchar(13),
    has_availability varchar(1),
    availability_30 smallint,
    availability_60 smallint,
    availability_90 smallint,
    availability_365 smallint,
    calendar_last_scraped varchar(10),
    number_of_reviews smallint,
    number_of_reviews_ltm smallint,
    first_review varchar(10),
    last_review varchar(10),
    review_scores_rating varchar(3),
    review_scores_accuracy varchar(2),
    review_scores_cleanliness varchar(2),
    review_scores_checkin varchar(2),
    review_scores_communication varchar(2),
    review_scores_location varchar(2),
    review_scores_value varchar(2),
    requires_license varchar(1),
    license varchar(14),
    jurisdiction_names varchar(48),
    instant_bookable varchar(1),
    is_business_travel_ready varchar(1),
    cancellation_policy varchar(27),
    require_guest_profile_picture varchar(1),
    require_guest_phone_verification varchar(1),
    calculated_host_listings_count smallint,
    calculated_host_listings_count_entire_homes smallint,
    calculated_host_listings_count_private_rooms smallint,
    calculated_host_listings_count_shared_rooms smallint,
    reviews_per_month varchar(5)
);
""")

staging_reviews_create = ("""
CREATE TEMPORARY TABLE IF NOT EXISTS staging_reviews (
    listing_id int,
    id int,
    date varchar(10),
    reviewer_id int,
    reviewer_name varchar(100),
    comments varchar(max)
);
""")

staging_restaurants_create = ("""
CREATE TEMPORARY TABLE IF NOT EXISTS staging_restaurants (
    address varchar(43),
    category varchar(64),
    id varchar(85),
    lat numeric,
    lng numeric,
    location varchar(15),
    name varchar(57),
    originalid varchar(51),
    polarity varchar(60),
    subcategory varchar(31),
    details varchar(50),
    reviews varchar(59)
);
""")

staging_attractions_create = ("""
CREATE TEMPORARY TABLE IF NOT EXISTS staging_attractions (
    address varchar(50),
    category varchar(63),
    id varchar(72),
    lat numeric,
    lng numeric,
    location varchar(15),
    name varchar(64),
    originalid varchar(51),
    polarity varchar(60),
    subcategory varchar(40),
    details varchar(50),
    reviews varchar(59)
);
""")


## target tables

airbnb_listings_create = ("""
CREATE TABLE IF NOT EXISTS airbnb_listings (
    listing_id int PRIMARY KEY,
    listing_name varchar NOT NULL,
    summary varchar(max),
    space varchar(max),
    description varchar(max),
    room_type varchar(20),
    price decimal NOT NULL,
    host_id int NOT NULL,
    neighbourhood varchar(100),
    neighbourhood_overview varchar(max),
    listing_lat decimal NOT NULL,
    listing_long decimal NOT NULL,
    city_id int NOT NULL
    );
""")

airbnb_hosts_create = ("""
CREATE TEMPORARY TABLE IF NOT EXISTS airbnb_hosts (
    host_id int PRIMARY KEY,
    host_name varchar NOT NULL,
    host_since date NOT NULL,
    host_location varchar(100),
    host_about varchar(max),
    host_response_time varchar(20),
    host_response_rate varchar(4),
    host_acceptance_rate varchar(4),
    host_is_superhost varchar(1),
    host_listings_count int,
    host_identity_verified varchar(1)
    );
""")

airbnb_reviews_create = ("""
CREATE TEMPORARY TABLE IF NOT EXISTS airbnb_reviews (
    review_id int PRIMARY KEY
    listing_id int NOT NULL,
    date date NOT NULL,
    reviewer_id int NOT NULL,
    reviewer_name varchar(100) NOT NULL,
    comments varchar(max) NOT NULL);
""")

cities_create = ("""
CREATE TEMPORARY TABLE IF NOT EXISTS cities (
    city_id IDENTITY(0,1) PRIMARY KEY,
    city_name varchar(100) NOT NULL,
    state varchar(100),
    country_code varchar(5) NOT NULL,
    country_name varchar(20) NOT NULL
);
""")

restaurants_create = ("""
CREATE TEMPORARY TABLE IF NOT EXISTS restaurants (
    restaurant_id int PRIMARY KEY,
    restaurant_name varchar NOT NULL,
    restaurant_category varchar,
    restaurant_lat decimal NOT NULL,
    restautant_long decimal NOT NULL,
    restaurant_address varchar,
    city_id int NOT NULL
);
""")

attractions_create = ("""
CREATE TEMPORARY TABLE IF NOT EXISTS attractions (
    attraction_id int PRIMARY KEY,
    attraction_name varchar NOT NULL,
    attraction_category varchar,
    attraction_lat decimal NOT NULL,
    restautant_long decimal NOT NULL,
    city_id int NOT NULL
);
""")

# STAGING TABLES

staging_listings_copy = ("""
COPY staging_listings
FROM '{}'
CREDENTIALS 'aws_iam_role={}' 
compupdate off region
ignoreheader 1
null as 'NA'
removequotes
delimiter ',';
""").format(LISTINGS_DATA, DWH_ROLE_ARN)

staging_reviews_copy = ("""
COPY staging_reviews
FROM '{}'
CREDENTIALS 'aws_iam_role={}' 
compupdate off region
ignoreheader 1
null as 'NA'
removequotes
delimiter ',';
""").format(REVIEWS_DATA, DWH_ROLE_ARN)

staging_restaurants_copy = ("""
COPY staging_restaurants
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
gzip delimiter ';' compupdate off region
REGION 'us-west-2';
""").format(RESTAURANTS_DATA, DWH_ROLE_ARN)

staging_attractions_copy = ("""
COPY staging_attractions
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
gzip delimiter ';' compupdate off region
REGION 'us-west-2';
""").format(ATTRACTIONS_DATA, DWH_ROLE_ARN)


# FINAL TABLES

cities_insert = ("""
INSERT INTO cities (city_name, state, country_code, country_name)
SELECT DISTINCT lower(city), lower(state), upper(country_code), lower(country)
FROM staging_listings
""")

airbnb_listings_insert = ("""
INSERT INTO airbnb_listings (listing_id, listing_name, summary, space, description, room_type, price, host_id,
                            neighbourhood, neighbourhood_overview, listing_lat, listing_long, city_id)
SELECT DISTINCT id, name, summary, space, description, room_type, price, host_id, neighbourhood_cleansed,
                neighborhood_overview, latitude, longitude, c.city_id
FROM staging_listings l
LEFT JOIN cities c ON lower(l.city) = c.city
""")

airbnb_hosts_insert = (""" 
INSERT INTO airbnb_hosts (host_id, host_name, host_since, host_location, host_about, host_response_time,
                            host_response_rate, host_acceptance_rate, host_is_superhost, host_listings_count, 
                            host_identity_verified)
SELECT DISTINCT host_id, host_name, to_date(host_since, 'YYYY-MM-DD'), host_location, host_about, host_response_time, host_response_rate, 
                host_acceptance_rate, host_is_superhost, host_listings_count, host_identity_verified
FROM staging_listings
""")

airbnb_reviews_insert = (""" 
INSERT INTO airbnb_reviews (review_id, listing_id, date, reviewer_id, reviewer_name, comments)
SELECT DISTINCT id, listing_id, to_date(date, 'YYYY-MM-DD'), reviewer_id, reviewer_name, comments
FROM staging_reviews
""")

restaurants_insert = ("""
INSERT INTO restaurants (restaurant_id, restaurant_name, restaurant_category, restaurant_lat, restaurant_long,
                        restaurant_address, city_id)
SELECT DISTINCT originalid, name, subcategory, lat, lng, address, c.city_id
FROM staging_restaurants r
JOIN cities c ON lower(r.location) = c.city_name
""")

attractions_insert = ("""
INSERT INTO attractions (attraction_id, attraction_name, attraction_category, attraction_lat, attraction_long, city_id)
SELECT DISTINCT originalid, name, subcategory, lat, lng, c.city_id
FROM staging_attractions a
JOIN cities c ON lower(a.location) = c.city_name
""")

# QUERY LISTS

create_table_queries = [staging_listings_create, staging_reviews_create, staging_restaurants_create, staging_attractions_create, 
                        airbnb_listings_create, airbnb_reviews_create, airbnb_hosts_create, cities_create, restaurants_create, attractions_create]
drop_table_queries = [staging_listings_drop, staging_reviews_drop, staging_restaurants_drop, staging_attractions_drop,
                      airbnb_listings_drop, airbnb_reviews_drop, airbnb_hosts_drop, cities_drop, restaurants_drop, attractions_drop]
copy_table_queries = [staging_listings_copy, staging_reviews_copy, staging_restaurants_copy, staging_attractions_copy]
insert_table_queries = [cities_insert, airbnb_listings_insert, airbnb_hosts_insert, airbnb_reviews_insert, restaurants_insert, attractions_insert]
