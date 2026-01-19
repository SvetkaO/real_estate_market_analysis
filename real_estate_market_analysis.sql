1 -- Market analysis by region and selling time --

-- Identify anomalous values (outliers) using percentile thresholds
WITH limits AS (
    SELECT
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_area) AS total_area_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS rooms_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony) AS balcony_limit,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_high,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_low
    FROM real_estate.flats
),

-- Select listing IDs without outliers (missing values are preserved)
filtered_id AS (
    SELECT id
    FROM real_estate.flats
    WHERE
        total_area < (SELECT total_area_limit FROM limits)
        AND (rooms < (SELECT rooms_limit FROM limits) OR rooms IS NULL)
        AND (balcony < (SELECT balcony_limit FROM limits) OR balcony IS NULL)
        AND (
            (ceiling_height < (SELECT ceiling_height_limit_high FROM limits)
             AND ceiling_height > (SELECT ceiling_height_limit_low FROM limits))
            OR ceiling_height IS NULL
        )
),

category AS (
    SELECT
        CASE
            WHEN c.city = 'Санкт-Петербург' THEN 'Saint Petersburg'
            ELSE 'Leningrad Region'
        END AS region,

        -- Categorization of listings by exposure duration
        CASE
            WHEN a.days_exposition <= 30 THEN 'Up to 1 month'
            WHEN a.days_exposition <= 90 THEN '1 to 3 months'
            WHEN a.days_exposition <= 180 THEN '3 to 6 months'
            WHEN a.days_exposition >= 181 THEN 'More than 6 months'
            ELSE 'No category'
        END AS sell_time,

        f.total_area,
        f.rooms,
        f.ceiling_height,
        f.balcony,
        f.floor,
        a.last_price,
        f.parks_around3000,
        f.ponds_around3000,
        f.airports_nearest,
        f.open_plan,
        f.is_apartment

    FROM real_estate.advertisement AS a
    JOIN real_estate.flats AS f ON a.id = f.id
    JOIN real_estate.city AS c ON f.city_id = c.city_id
    JOIN real_estate.type AS t ON f.type_id = t.type_id
    WHERE a.id IN (SELECT id FROM filtered_id)
      AND DATE_TRUNC('year', a.first_day_exposition)
          BETWEEN DATE '2015-01-01' AND DATE '2018-01-01'
      AND t.type = 'город'
)

SELECT
    region,
    sell_time,
    COUNT(*) AS total_sold_apartments, -- number of sold apartments
    ROUND(AVG(total_area)::NUMERIC, 2) AS avg_total_area,
    ROUND(AVG(last_price / total_area)::NUMERIC, 2) AS avg_price_per_sqm,
    ROUND(AVG(rooms)::NUMERIC, 2) AS avg_rooms,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY rooms) AS rooms_median,
    ROUND(AVG(ceiling_height)::NUMERIC, 2) AS avg_ceiling_height,
    ROUND(AVG(balcony)::NUMERIC, 2) AS avg_balcony,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY balcony) AS balcony_median,
    ROUND(AVG(floor)::NUMERIC, 2) AS avg_floor,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY parks_around3000) AS parks_median,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY ponds_around3000) AS ponds_median,
    ROUND(AVG(airports_nearest)::NUMERIC, 2) AS avg_distance_to_airport,
    ROUND(SUM(open_plan) * 100.0 / COUNT(*), 2) AS share_of_open_plan,
    ROUND(SUM(is_apartment) * 100.0 / COUNT(*), 2) AS share_of_apartments,
    ROUND(SUM(CASE WHEN rooms = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(rooms), 2)
        AS share_of_studios
FROM category
GROUP BY region, sell_time
ORDER BY region DESC;


2 -- Seasonality analysis --


-- Identify outliers using percentile thresholds
WITH limits AS (
    SELECT
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_area) AS total_area_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS rooms_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony) AS balcony_limit,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_high,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_low
    FROM real_estate.flats
),

-- Select listings without outliers
filtered_id AS (
    SELECT id
    FROM real_estate.flats
    WHERE
        total_area < (SELECT total_area_limit FROM limits)
        AND (rooms < (SELECT rooms_limit FROM limits) OR rooms IS NULL)
        AND (balcony < (SELECT balcony_limit FROM limits) OR balcony IS NULL)
        AND (
            (ceiling_height < (SELECT ceiling_height_limit_high FROM limits)
             AND ceiling_height > (SELECT ceiling_height_limit_low FROM limits))
            OR ceiling_height IS NULL
        )
),

extract_month AS (
    SELECT
        a.id,
        f.total_area,
        a.last_price,
        EXTRACT(MONTH FROM a.first_day_exposition) AS publish_month,
        a.days_exposition,
        EXTRACT(
            MONTH FROM a.first_day_exposition
            + (a.days_exposition || ' days')::INTERVAL
        ) AS sell_month
    FROM real_estate.advertisement AS a
    JOIN real_estate.flats AS f ON a.id = f.id
    JOIN real_estate.city AS c ON f.city_id = c.city_id
    JOIN real_estate.type AS t ON f.type_id = t.type_id
    WHERE a.id IN (SELECT id FROM filtered_id)
      AND DATE_TRUNC('year', a.first_day_exposition)
          BETWEEN DATE '2015-01-01' AND DATE '2018-01-01'
      AND t.type = 'город'
),

-- Statistics by listing publication month
publish_month AS (
    SELECT
        RANK() OVER (ORDER BY COUNT(publish_month) DESC) AS publish_rank,
        publish_month AS month,
        COUNT(*) AS total_published,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS publish_share,
        ROUND(AVG(total_area)::NUMERIC, 2) AS avg_total_area,
        ROUND(AVG(last_price / total_area)::NUMERIC, 2) AS avg_price_per_sqm
    FROM extract_month
    GROUP BY publish_month
),

-- Statistics by listing removal (sale) month
sold_month AS (
    SELECT
        RANK() OVER (ORDER BY COUNT(sell_month) DESC) AS sold_rank,
        sell_month AS month,
        COUNT(*) AS total_sold,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS sold_share,
        ROUND(AVG(total_area)::NUMERIC, 2) AS avg_total_area,
        ROUND(AVG(last_price / total_area)::NUMERIC, 2) AS avg_price_per_sqm
    FROM extract_month
    GROUP BY sell_month
)

SELECT
    p.publish_rank,
    s.sold_rank,
    COALESCE(p.month, s.month) AS month,
    COALESCE(p.total_published, 0) AS total_published,
    COALESCE(s.total_sold, 0) AS total_sold,
    p.avg_total_area AS avg_total_area_published,
    p.avg_price_per_sqm AS avg_price_per_sqm_published,
    s.avg_total_area AS avg_total_area_sold,
    s.avg_price_per_sqm AS avg_price_per_sqm_sold,
    p.publish_share,
    s.sold_share
FROM publish_month p
FULL OUTER JOIN sold_month s ON p.month = s.month
ORDER BY month;
