-- ################################################
-- ### CREATING DATABASE AND ACCESS RIGHTS      ###
-- ################################################
-- creating database on external disks
-- 1. mkdir /mnt/mssql/migration
-- 2. chmod o+w /mnt/mssql/migration
-- still AS DB root user
CREATE DATABASE migration ON (
    NAME = 'migration',
    FILENAME = '/mnt/mssql/migration/migration.mdf'
) LOG ON (
    NAME = 'migration_log',
    FILENAME = '/mnt/mssql/migration/migration.ldf'
);

-- set recOVERy model to simple 
ALTER DATABASE migration
SET
    RECOVERY SIMPLE;

-- switch to database
USE migration;

-- CREATE user for database and assign owner rights
CREATE USER bokanyie FOR LOGIN bokanyie;

ALTER ROLE [db_owner]
ADD
    MEMBER bokanyie;

ALTER ROLE [db_datawriter]
ADD
    MEMBER bokanyie;

ALTER ROLE [db_datareader]
ADD
    MEMBER bokanyie;

-- ################################################
-- ### INSERTING DATA                           ###
-- ################################################
/*
 wget https://api.gbif.org/v1/occurrence/download/request/0116153-210914110416597.zip
 
 */
-- preparing the place for the data insertion
CREATE TABLE birds_small (
    [gbifID] VARCHAR (256) NOT NULL,
    [species] VARCHAR (256) NOT NULL,
    [decimalLatitude] FLOAT NOT NULL,
    [decimalLongitude] FLOAT NOT NULL,
    [coordinateUncertaintyInMeters] FLOAT NOT NULL,
    [eventDate] DATETIME NOT NULL,
    [day] INT NOT NULL,
    [month] INT NOT NULL,
    [year] INT NOT NULL,
    [OrganismID] VARCHAR (256) NOT NULL,
    -- unique identifiers for the 20 birds observed
    PRIMARY KEY (OrganismID, gbifID)
);


-- making the insert
--(unfortunately, ON Linux MSSQL server, this can only be done WITH sysadmin user)
BULK
INSERT
    birds_small
FROM
    '/mnt/data/bird_migration/birb20indcropped_min_corr.tsv' WITH (
        FIELDTERMINATOR = ',',
        FIRSTROW = 2,
        -- skipping header row
        ROWTERMINATOR = '0x0a' -- might be different for different OS/encodings
    );


-- how does it look like?
SELECT
    TOP 10 *
FROM
    birds_small;


-- which identifier to use, how many lines  birds do we have?
WITH temp1 as(
    SELECT
        distinct gbifID
    FROM
        birds_small
),
temp2 AS (
    SELECT
        distinct OrganismID
    FROM
        birds_small
)
SELECT
    'num_lines' AS [description],
    COUNT(*) AS cnt
FROM
    birds_small
union
all
SELECT
    'distinct_gbifID' AS [description],
    COUNT(*) AS cnt
FROM
    temp1
union
all
SELECT
    'distinct_OrganismID' AS [description],
    COUNT(*) AS cnt
FROM
    temp2;
    

-- ################################################
-- ### ARRANGING DATA                           ###
-- ################################################

-- unfortunately, the dataset is not yet ordered by date within one bird
SELECT
    eventDate,
    lag(eventDate) OVER (
        PARTITION BY OrganismID
        ORDER BY
            gbifID
    ) AS previous_eventDate
FROM
    birds_small;


-- first, we should create a column ON which it will be faster to calculate / order
-- testing Unix epoch conversion which is somewhat unintuitive bc of being bigint
SELECT
    TOP 10 CONVERT(BIGINT, DATEDIFF(DAY, '19700101', eventDate)) * 86400,
    -- years since epoch * seconds in a year
    DATEDIFF(
        second,
        DATEADD(DAY, DATEDIFF(DAY, 0, eventDate), 0),
        eventDate
    ),
    -- difference in seconds between midnight of SELECTed date and SELECTed date
    DATEADD(DAY, DATEDIFF(DAY, 0, eventDate), 0),
    -- midnight ON SELECTed day
    DATEDIFF(DAY, 0, eventDate),
    -- number of days between 0 and SELECTed day
    CONVERT(BIGINT, DATEDIFF(DAY, '19700101', eventDate)) * 86400 + DATEDIFF(
        second,
        DATEADD(DAY, DATEDIFF(DAY, 0, eventDate), 0),
        eventDate
    ) AS event_timestamp
FROM
    birds_small;

    
-- store and sort new dataset
CREATE TABLE birds_small_timestamped (
    [OrganismID] VARCHAR (256) NOT NULL,
    [event_timestamp] BIGINT NOT NULL,
    [gbifID] VARCHAR (256) NOT NULL,
    [species] VARCHAR (256) NOT NULL,
    [decimalLatitude] FLOAT NOT NULL,
    [decimalLongitude] FLOAT NOT NULL,
    [coordinateUncertaintyInMeters] FLOAT NOT NULL,
    [eventDate] DATETIME NOT NULL,
    [day] INT NOT NULL,
    [month] INT NOT NULL,
    [year] INT NOT NULL
    PRIMARY KEY(OrganismID, event_timestamp)
);


INSERT INTO
    birds_small_timestamped WITH(TABLOCKX)
SELECT
    [OrganismID],
    CONVERT(BIGINT, DATEDIFF(DAY, '19700101', eventDate)) * 86400 + DATEDIFF(
        second,
        DATEADD(DAY, DATEDIFF(DAY, 0, eventDate), 0),
        eventDate
    ),
    [gbifID],
    [species],
    [decimalLatitude],
    [decimalLongitude],
    [coordinateUncertaintyInMeters],
    [eventDate],
    [day],
    [month],
    [year]
FROM
    birds_small;


-- have we done everything all right?
SELECT
    TOP 10 *
FROM
    birds_small_timestamped;


-- let's have a look at some of the basics of the dataset
SELECT
    year,
    COUNT(*) AS cnt
FROM
    birds_small_timestamped
GROUP BY
    year
ORDER BY
    1 ASC;


SELECT
    month,
    COUNT(*) AS cnt
FROM
    birds_small_timestamped
GROUP BY
    month
ORDER BY
    1 ASC;


SELECT
    [OrganismID],
    COUNT(*) AS cnt
FROM
    birds_small_timestamped
GROUP BY
    [OrganismID];


-- ################################################
-- ### CHAINING CONSECUTIVE EVENTS              ###
-- ################################################

CREATE TABLE birds_small_consecutive_events (
        [OrganismID] VARCHAR (256) NOT NULL,
        event_timestamp BIGINT NOT NULL,
        previous_event_timestamp BIGINT,
        lat FLOAT NOT NULL,
        lon FLOAT NOT NULL,
        previous_lat FLOAT,
        previous_lon FLOAT
        PRIMARY KEY(OrganismID, event_timestamp)
    )
INSERT INTO
    birds_small_consecutive_events WITH(TABLOCKX)
SELECT
    [OrganismID],
    event_timestamp,
    lag(event_timestamp) OVER (
        PARTITION BY OrganismID
        ORDER BY
            event_timestamp
    ),
    decimalLatitude AS lat,
    decimalLongitude AS lon,
    lag(decimalLatitude) OVER (
        PARTITION BY OrganismID
        ORDER BY
            event_timestamp
    ),
    lag(decimalLongitude) OVER (
        PARTITION BY OrganismID
        ORDER BY
            event_timestamp
    )
FROM
    birds_small_timestamped;


-- check if we were all right
SELECT
    TOP 10 *
FROM
    birds_small_consecutive_events;
    
-- calculate distance and time difference, and velocity between consecutive events
CREATE TABLE birds_consecutive_events_delta (
    [OrganismID] VARCHAR (256) NOT NULL,
    event_timestamp BIGINT NOT NULL,
    delta_t INT NOT NULL,
    distance FLOAT NOT NULL,
    velocity FLOAT NOT NULL
    PRIMARY KEY(OrganismID, event_timestamp)
);


INSERT INTO
    birds_consecutive_events_delta WITH(TABLOCKX)
SELECT
    [OrganismID],
    event_timestamp,
    event_timestamp - previous_event_timestamp AS delta_t,
    geography :: Point(lat, lon, 4326).STDistance(geography :: Point(previous_lat, previous_lon, 4326)) / 1000 AS distance_km,
    geography :: Point(lat, lon, 4326).STDistance(geography :: Point(previous_lat, previous_lon, 4326)) /(event_timestamp - previous_event_timestamp) * 3.6 AS velocity_kmh
FROM
    birds_small_consecutive_events
WHERE
    previous_event_timestamp is NOT NULL
    and previous_lat is NOT NULL
    and previous_lon is NOT NULL;


-- ################################################
-- ### STATISTICS                               ###
-- ################################################
-- min, max, average delta_t, distance and velocity per bird
SELECT
    [OrganismID],
    min(delta_t) AS min_delta_t,
    max(delta_t) AS max_delta_t,
    avg(delta_t) AS avg_delta_t,
    min(distance) AS min_distance_km,
    max(distance) AS max_distance_km,
    avg(distance) AS avg_distance_km,
    min(velocity) AS min_velocity,
    max(velocity) AS max_velocity,
    avg(velocity) AS avg_velocity
FROM
    birds_consecutive_events_delta
GROUP BY
    [OrganismID];

    
-- velocity distribution
DECLARE @bw FLOAT = 10
SELECT
    @bw * floor(velocity / @bw),
    COUNT(*)
FROM
    birds_consecutive_events_delta
GROUP BY
    @bw * floor(velocity / @bw)
ORDER BY
    1;


-- distance distribution
DECLARE @bw FLOAT = 1
SELECT
    @bw * floor(distance / @bw),
    COUNT(*)
FROM
    birds_consecutive_events_delta
GROUP BY
    @bw * floor(distance / @bw)
ORDER BY
    1;


-- selecting parameters to chain consecutive events
DECLARE @delta_t_threshold FLOAT = 30 * 60
DECLARE @velocity_threshold FLOAT = 60
DECLARE @distance_threshold FLOAT = 1

CREATE TABLE birds_stop_detection_temp (
    [OrganismID] VARCHAR (256) NOT NULL,
    event_timestamp BIGINT NOT NULL,
    small_delta_t INT NOT NULL,
    small_delta_d INT NOT NULL,
    both_small INT NOT NULL,
    valid_velocity INT NOT NULL
    PRIMARY KEY(OrganismID, event_timestamp)
);


INSERT INTO
    birds_stop_detection_temp WITH(TABLOCKX)
SELECT
    [OrganismID],
    event_timestamp,
    (sign(@delta_t_threshold - delta_t) + 1) / 2 AS small_delta_t, -- is delta_t < threshold? if yes, 1
    (sign(@distance_threshold - distance) + 1) / 2 AS small_delta_d, -- is delta_d < threshold? if yes, 1
    1 -(
        sign(
            (
                (sign(@delta_t_threshold - delta_t) + 1) / 2 +(sign(@distance_threshold - distance) + 1) / 2
            ) / 2 -0.75
        ) + 1
    ) / 2 AS both_small, -- is delta_t < threshold AND delta_d < threshold? if yes, 0, else 1
    (sign(@velocity_threshold - velocity) + 1) / 2 AS valid_velocity -- is velocity < threshold? if yes, 1
FROM
    birds_consecutive_events_delta
WHERE
    (sign(@velocity_threshold - velocity) + 1) / 2 = 1;


SELECT TOP 10 * FROM birds_stop_detection_temp;


CREATE TABLE birds_stop_detection_temp2 (
    [OrganismID] VARCHAR (256) NOT NULL,
    event_timestamp BIGINT NOT NULL,
    both_small INT NOT NULL,
    stopid INT NOT NULL,
    lat FLOAT NOT NULL,
    lon FLOAT NOT NULL
    PRIMARY KEY(OrganismID, stopid)
);


INSERT INTO
    birds_stop_detection_temp2 WITH(TABLOCKX)
SELECT
    t.[OrganismID],
    t.event_timestamp,
    both_small,
    sum(both_small) OVER(
        PARTITION BY t.[OrganismID]
        ORDER BY
            t.event_timestamp
    ) AS stopid,
    ce.lat,
    ce.lon
FROM
    birds_stop_detection_temp t
    inner JOIN birds_small_consecutive_events ce ON t.OrganismID = ce.OrganismID
    and t.event_timestamp = ce.event_timestamp;


SELECT TOP 10  
    * 
FROM 
    birds_stop_detection_temp2;

    
-- ################################################
-- ### FINAL TABLE WITH STOPS                   ###
-- ################################################

CREATE TABLE birds_stop_locations (
    [OrganismID] VARCHAR (256) NOT NULL,
    stopid INT NOT NULL,
    min_ts BIGINT NOT NULL,
    max_ts BIGINT NOT NULL,
    duration_h FLOAT NOT NULL,
    latc FLOAT NOT NULL,
    lonc FLOAT NOT NULL
    PRIMARY KEY(OrganismID, stopid)
);


INSERT INTO
    birds_stop_locations WITH(TABLOCKX)
SELECT
    [OrganismID],
    stopid,
    min(event_timestamp) AS min_ts,
    max(event_timestamp) AS max_ts,
    round(
        cast(
            max(event_timestamp) - min(event_timestamp) AS FLOAT
        ) / 3600,
        1
    ) AS duration_h,
    avg(lat) AS latc,
    avg(lon) AS lonc
FROM
    birds_stop_detection_temp2
WHERE
    both_small = 0
GROUP BY
    [OrganismID],
    stopid;