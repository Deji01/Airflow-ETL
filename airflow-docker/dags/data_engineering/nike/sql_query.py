create_table_query = """
    CREATE TABLE IF NOT EXISTS nike (
    date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    id VARCHAR(50) NOT NULL PRIMARY KEY,
    pid VARCHAR(10),
    product_id VARCHAR(50),
    product_instance_id VARCHAR(50),
    product_type VARCHAR(20),
    title VARCHAR(100),
    subtitle VARCHAR(100),
    color_description VARCHAR(255),
    currency VARCHAR(10),
    current_price NUMERIC(10, 2),
    discounted BOOLEAN,
    employee_price NUMERIC(10, 2),
    full_price NUMERIC(10, 2),
    minimum_advertised_price NUMERIC(10, 2),
    label VARCHAR(20),
    in_stock BOOLEAN,
    is_coming_soon BOOLEAN,
    is_best_seller BOOLEAN,
    is_excluded BOOLEAN,
    is_gift_card BOOLEAN,
    is_jersey BOOLEAN,
    is_launch BOOLEAN,
    is_member_exclusive BOOLEAN,
    is_nba BOOLEAN,
    is_nfl BOOLEAN,
    is_sustainable BOOLEAN,
    has_extended_sizing BOOLEAN,
    customizable BOOLEAN,
    search_term VARCHAR(20),
    portrait_url VARCHAR(255),
    squarish_url VARCHAR(255),
    url VARCHAR(255)
    )
    """

insert_data_query = """
    INSERT INTO nike (
        id,
        pid,
        product_id,
        product_instance_id,
        product_type,
        title,
        subtitle,
        color_description,
        currency,
        current_price,
        discounted,
        employee_price,
        full_price,
        minimum_advertised_price,
        label,
        in_stock,
        is_coming_soon,
        is_best_seller,
        is_excluded,
        is_gift_card,
        is_jersey,
        is_launch,
        is_member_exclusive,
        is_nba,
        is_nfl,
        is_sustainable,
        has_extended_sizing,
        customizable,
        search_term,
        portrait_url,
        squarish_url,
        url
        )
    VALUES (
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s
    )
    """

