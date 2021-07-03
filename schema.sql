create table launch_point (
    id integer primary key,
    site_name varchar,
    lat float,
    lon float,
    height_amsl float,

    -- The ID used to identify this launch point on the BGA ladder
    ladder_id int unique,

    -- Three character code that the BGA ladder uses to identify the club that
    -- flies out of this site
    club_ladder_code varchar
);

create table club (
    id integer primary key,
    
    club_name varchar not null,
    is_university boolean,

    -- Three character code that the BGA ladder uses to identify this club
    ladder_code varchar unique
);

create table pilot (
    id integer primary key,
    forename varchar,
    surname varchar,

    -- The ID used to identify this pilot on the BGA ladder
    ladder_id int unique
);

create table turnpoint (
    id integer primary key,

    -- The canonical three character code identifying this turnpoint
    code varchar not null unique,
    
    lat float,
    lon float,

    -- Height above mean sea level in meters
    height_amsl float,
    
    -- Human readable description of the turnpoint
    helper_text varchar
);

create table glider_model (
    id integer primary key,
    
    model_name varchar unique,
    seats int,
    vintage boolean,
    turbo boolean,
    handicap float,

    --- The ID used to identify this glider model on the BGA ladder
    ladder_id int unique
);

create table glider (
    id integer primary key,

    reg varchar unique,
    model int,
    
    foreign key (model) references glider_model (id)
);

create table trace (
    id integer primary key,
    
    -- The timestamp this file was downloaded
    downloaded_at timestamp not null,
    
    -- The filename originally assigned to this trace
    original_filename varchar,
    
    -- Checksum of the file
    sha256_hash varchar unique not null
);

create table task (
    -- Unique ID for this task
    id int,

    -- The index of this turnpoint within the task
    turnpoint_index int,

    -- Usually a three letter string describing the turnpoint
    turnpoint_code varchar not null,
    
    primary key (id, turnpoint_index)
);

create index task_id_index on task (id);

create table flight (
    id integer primary key,

    pilot int,
    club int,
    glider int,
    trace int,
    flight_date timestamp,
    scraped_at timestamp,
    
    is_weekend boolean,
    is_junior boolean,
    is_height boolean,
    is_two_seater boolean,
    is_wooden boolean,
    has_engine boolean,
    penalty boolean,
    task int,

    speed float,
    handicap_speed float,
    scoring_distance float,
    speed_points int,
    height_gain int,
    height_points int,
    total_points int,
    
    --- The ID used to identify this glider model on the BGA ladder
    ladder_id int unique,

    foreign key (pilot) references pilot (id),
    foreign key (club) references club (id),
    foreign key (glider) references glider (id),
    foreign key (trace) references trace (id),
    foreign key (task) references task (id)
);