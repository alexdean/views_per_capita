require 'csv'
require 'pg'

def db_name
  'views_per_capita'
end

def db
  @db ||= PG.connect(dbname: db_name)
end

def srid
  4326
end

def table_exists?(table_name)
  result = db.exec "select count(*) as table_exists from pg_tables where tablename = '#{table_name}'"
  result[0]['table_exists'] == '0' ? false : true
end

def load_shp_file(table:, source_zip:)
  db.exec "DROP TABLE IF EXISTS #{table}"

  ext = File.extname(source_zip)
  base = File.basename(source_zip, ext)

  tmp_dir = "tmp/#{base}"
  out_sql = "tmp/#{table}.sql"

  `rm -Rf #{tmp_dir}`
  `unzip -o -d #{tmp_dir} #{source_zip}`
  `shp2pgsql -s #{srid} -t 2D -W LATIN1 -I #{tmp_dir}/#{base}.shp #{table} > #{out_sql}`
  `psql #{db_name} < #{out_sql}`
end

root_dir = Dir.getwd

task :setup do
  result = PG.connect.exec "select count(*) as db_exists from pg_database where datname = '#{db_name}'"
  if result[0]['db_exists'] == '0'
    `createdb #{db_name}`
    `echo 'create extension postgis' | psql #{db_name}`
  else
    puts "Database #{db_name} already exists. Not creating."
  end

  # http://www.faqoverflow.com/gis/16374.html
  sql = <<-EOF
  CREATE OR REPLACE FUNCTION ST_CreateFishnet(
          nrow integer, ncol integer,
          xsize float8, ysize float8,
          x0 float8 DEFAULT 0, y0 float8 DEFAULT 0,
          OUT "row" integer, OUT col integer,
          OUT the_geom geometry)
      RETURNS SETOF record AS
  $$
  SELECT i + 1 AS row, j + 1 AS col, ST_Translate(the_geom, j * $3 + $5, i * $4 + $6) AS the_geom
  FROM
    generate_series(0, $1 - 1) AS i,
    generate_series(0, $2 - 1) AS j,
    (
      SELECT ('POLYGON((0 0, 0 '||$4||', '||$3||' '||$4||', '||$3||' 0,0 0))')::geometry AS the_geom
    ) AS foo;
  $$ LANGUAGE sql IMMUTABLE STRICT;
  EOF
  db.exec sql
end

desc "create the view_locations table if it doesn't exist"
task create_view_locations: :setup do
  if ! table_exists?('view_locations')
    sql = <<-EOF
    CREATE TABLE view_locations (
      id integer primary key,
      total_views integer,
      grid_square_id integer
    );
    EOF
    db.exec sql
    db.exec "SELECT AddGeometryColumn('view_locations', 'the_geom', #{srid}, 'POINT', 2, false)"
    db.exec "create index on view_locations using gist (the_geom)"
    db.exec "create index on view_locations (grid_square_id)"
  end
end

desc "load view_locations data from TED videometrics csv exports"
task load_view_locations: :create_view_locations do
  db.exec "DELETE FROM view_locations"

  # from videometrics database
  # SELECT id, latitude, longitude FROM view_locations WHERE country_id = 10;
  # export to view_locations.csv
  location_csv = 'source_data/view_locations.csv'
  # SELECT location_id, count(*) as total_views FROM events WHERE happened_at BETWEEN '2015-01-01 08:00:00' AND '2015-06-01 07:59:59' GROUP BY location_id;
  views_per_location_csv = 'source_data/views_per_location.csv'

  sql_file = 'tmp/view_locations.sql'

  views_per_location = {}
  CSV.foreach(views_per_location_csv, headers: true) do |line|
    views_per_location[line['location_id'].to_i] = line['total_views']
  end

  out = File.open(sql_file, 'w')
  CSV.foreach(location_csv, headers: true) do |line|
    sql = <<-EOF
      INSERT INTO view_locations (id, total_views, the_geom)
      VALUES (
        #{line['id']},
        #{views_per_location[line['id'].to_i].to_i},
        ST_SetSRID(ST_Point(#{line['longitude']}, #{line['latitude']}), #{srid}));
    EOF
    out.write(sql.gsub(/\n */, " ").strip+"\n")
  end
  out.close

  `psql #{db_name} < #{sql_file}`
end

desc "create the grid_squares table if it doesn't exist."
task create_grid_squares: :setup do
  if ! table_exists?('grid_squares')
    sql = <<-EOF
      CREATE TABLE grid_squares (
        id serial primary key,
        name varchar(255),
        population integer,
        total_views integer,
        views_per_capita numeric (10,5)
      )
    EOF
    db.exec sql

    db.exec "SELECT AddGeometryColumn('grid_squares', 'the_geom', #{srid}, 'POLYGON', 2);"
    db.exec "CREATE INDEX ON grid_squares USING GIST (the_geom);"
  end
end

desc "populate (or re-populate) the grid_squares table, and calculate grid_square population from county data."
task load_grid_squares: [:create_grid_squares, :load_counties] do
  # build a grid that covers the entire area where we have view_locations
  # parameters determined manually.
  db.exec "delete from grid_squares"
  db.exec "insert into grid_squares (the_geom) select ST_SetSRID(the_geom, #{srid}) from ST_CreateFishnet(57, 115, 1, 1, -180, 15)"

  # remove grid_squares which have no view_locations
  # should be able to do this with one query, but getting unexpected results
  # so opting for the dumb/procedural approach instead.
  # run select to see. (many (all?) grid_squares join to a location with NULL id, which doesn't exist in view_locations table.)
  # delete from grid_squares where id IN (select c.id from grid_squares c left join view_locations l ON (ST_Contains(c.geom, l.point)) group by c.id having count(*) = 1);
  no_view_locations = []
  result = db.exec("select id from grid_squares")
  result.each do |row|
    sql = <<-EOF
      SELECT count(*) as location_count
      FROM grid_squares gs
        LEFT JOIN view_locations l ON ST_Contains(gs.the_geom, l.the_geom)
      WHERE
        gs.id = #{row['id']}
        AND l.id IS NOT NULL
    EOF
    result = db.exec(sql)
    if result[0]['location_count'].to_i == 0
      no_view_locations << row['id']
    end
  end

  if no_view_locations.size > 0
    db.exec("DELETE FROM grid_squares WHERE id IN (#{no_view_locations.join(',')})")
  end

  grid_square_count = db.exec("select count(*) as grid_square_count from grid_squares")[0]['grid_square_count'].to_f

  result = db.exec("select id from grid_squares")
  result.each_with_index do |row, idx|
    # find counties which overlap this grid_square.
    # calculate % of the county which falls within the grid_square.
    # apply that % of the county's population to the grid_square.
    # (assume the county's population is evenly distributed.)
    sql = <<-EOF
      SELECT
        co.name,
        co.pop_est_2014,
        ST_Area(ST_Intersection(gs.the_geom, co.geom)) as overlap_area,
        ST_Area(co.geom) as total_county_area
      FROM grid_squares gs
        INNER JOIN counties co ON ST_Intersects(gs.the_geom, co.geom)
      WHERE
        gs.id = #{row['id']}
    EOF

    grid_square_population = 0
    result2 = db.exec(sql)
    result2.each do |county|
      pct_of_county_in_grid_square = county['overlap_area'].to_f / county['total_county_area'].to_f
      pct_of_county_population_in_grid_square = pct_of_county_in_grid_square * county['pop_est_2014'].to_i
      grid_square_population += pct_of_county_population_in_grid_square.to_i
    end

    sql = "UPDATE grid_squares SET population = #{grid_square_population} WHERE id = #{row['id']}"
    db.exec sql

    if idx > 0 && idx % 10 == 0
      print '.'

      if idx % 100 == 0
        print " #{(idx/grid_square_count*100).round}%"
        puts
      end
    end
  end

  puts
end

population_updates_base = 'CO-EST2014-alldata'
population_updates_file = "source_data/#{population_updates_base}.csv"
population_updates_source_url = "http://www.census.gov/popest/data/counties/totals/2014/files/#{population_updates_base}.csv"
desc "fetch population updates 2010-2014"
file population_updates_file do
  if ! File.exist?(population_updates_file)
    `wget -O #{population_updates_file} #{population_updates_source_url}`
  end

  if ! File.exist?(population_updates_file) || File.size(population_updates_file) == 0
    puts "#{population_updates_file} is missing. Downloading from #{population_updates_source_url} failed."
    puts "Try visting http://www.census.gov/popest/data/counties/totals/2014/CO-EST2014-alldata.html to find a replacement."
    exit 1
  end
end
desc "fetch csv of county population updates"
task population_updates_file: population_updates_file

counties_base = 'County_2010Census_DP1'
counties_zip_file = "source_data/#{counties_base}.zip"
counties_source_url = "http://www2.census.gov/geo/tiger/TIGER2010DP1/#{counties_base}.zip"
file counties_zip_file do
  `wget -O #{counties_zip_file} #{counties_source_url}`

  if ! File.exist?(counties_zip_file) || File.size(counties_zip_file) == 0
    puts "#{counties_zip_file} is missing, and downloading from #{counties_source_url} failed."
    puts "Try downloading a copy from the '2010 Census' > 'Counties' section of http://www.census.gov/geo/maps-data/data/tiger-data.html"
    puts
    puts "If this file is no longer available:"
    puts "  - fetch the closest equivalent"
    puts "  - save it (in zip format) into the source_data/ directory"
    puts "  - adjust the `counties_base` variable in this Rakefile"
    puts "  - inspect the data to see if the RENAME commands in the `load_counties` task need to be adjusted."
    puts "    the zip file should contain a spreadsheet describing the actual columns in the dbf data file"
    exit 1
  end
end
desc "fetch county definitions from Census Bureau website"
task counties_zip_file: counties_zip_file

desc "create (or re-create) the counties table and load data from Census Bureau shapefile"
task load_counties: [counties_zip_file, population_updates_file] do
  load_shp_file(table: 'counties', source_zip: counties_zip_file)
  db.exec "ALTER TABLE counties RENAME COLUMN dp0010001 TO total_population"
  db.exec "ALTER TABLE counties RENAME COLUMN namelsad10 TO name"
  db.exec "ALTER TABLE counties ADD pop_est_2010 integer, ADD pop_est_2014 integer"

  CSV.foreach(population_updates_file, encoding: "iso-8859-1:UTF-8", headers: true) do |line|
    fips = line['STATE'] + line['COUNTY']
    sql = <<-EOF
      UPDATE counties
      SET
        pop_est_2010 = #{line['POPESTIMATE2010']},
        pop_est_2014 = #{line['POPESTIMATE2014']}
      WHERE geoid10 = '#{fips}'
    EOF
    # puts line['STNAME'] + ' ' + line['CTYNAME']
    # puts sql
    db.exec sql
  end
end

states_base = "cb_2013_us_state_500k"
states_zip_file = "source_data/#{states_base}.zip"
states_source_url = "http://www2.census.gov/geo/tiger/GENZ2013/#{states_base}.zip"
file states_zip_file do
  `wget -O #{states_zip_file} #{states_source_url}`
  if ! File.exist?(states_zip_file) || File.size(states_zip_file) == 0
    puts "#{states_zip_file} is missing. Downloading from #{states_source_url} failed."
    puts "Try visiting http://www.census.gov/geo/maps-data/data/cbf/cbf_state.html to find a replacement."
    exit 1
  end
end
desc "fetch state outlines shapefile from US Census Bureau"
task states_zip_file: states_zip_file

desc "create (or re-create) the states table"
task load_states: states_zip_file do
  load_shp_file(table: 'states', source_zip: states_zip_file)
end

cities_tarball_base = "citiesx010g_shp_nt00962"
cities_tarball = "source_data/#{cities_tarball_base}.tar.gz"
cities_shpfile_base = "citiesx010g"
cities_source_url = "http://dds.cr.usgs.gov/pub/data/nationalatlas/#{cities_tarball_base}.tar.gz"
file cities_tarball do
  `wget -O #{cities_tarball} #{cities_source_url}`
  if ! File.exist?(cities_tarball) || File.size(cities_tarball) == 0
    puts "#{cities_tarball} is missing. Downloading from #{cities_source_url} failed."
    puts "Try visiting http://catalog.data.gov/dataset/cities-and-towns-of-the-united-states-direct-download/resource/f9fefcc9-5d26-437a-8435-9e388faded0f to find a replacement."
    exit 1
  end
end

desc "create (or re-create) the states table"
task load_cities: cities_tarball do
  table_name = 'cities'
  db.exec "DROP TABLE IF EXISTS #{table_name}"

  tmp_dir = "tmp/#{cities_tarball_base}"
  out_sql = "tmp/#{table_name}.sql"

  # TODO merge this with load_shp_file
  # problems: untarring doesn't create a subdir. files are untarred into the current dir.
  `rm -Rf #{tmp_dir}`
  `mkdir -p #{tmp_dir}`
  `cp #{cities_tarball} #{tmp_dir}`
  Dir.chdir tmp_dir do
    `tar xzvf ../../#{cities_tarball}`
    `shp2pgsql -s #{srid} -t 2D -W LATIN1 -I #{cities_shpfile_base}.shp #{table_name} > #{File.join(root_dir,out_sql)}`
  end
  `psql #{db_name} < #{File.join(root_dir,out_sql)}`
end

desc "populate view_locations.grid_square_id foreign key, describing which grid_square each view_location is within"
task associate_grid_squares_and_view_locations: [:load_view_locations, :load_grid_squares] do
  sql = <<-EOF
    SELECT
      gs.id,
      array_to_string(array_agg(l.id), ',') as location_ids
    FROM grid_squares gs
      LEFT JOIN view_locations l ON ST_Contains(gs.the_geom, l.the_geom)
    GROUP BY
      gs.id
  EOF
  result = db.exec sql
  result.each do |row|
    db.exec("UPDATE view_locations SET grid_square_id = #{row['id']} WHERE id IN (#{row['location_ids']})")
  end
end

desc "calcuate views per capita for all grid grid_squares"
task calculate_views_per_capita: :associate_grid_squares_and_view_locations do
  sql = <<-EOF
    UPDATE grid_squares c
    SET total_views = x.total_views
    FROM (
      SELECT
        gs.id,
        SUM(l.total_views) as total_views
      FROM grid_squares gs
        INNER JOIN view_locations l ON (gs.id = l.grid_square_id)
      GROUP BY
        gs.id
    ) x
    WHERE c.id = x.id
  EOF
  db.exec sql

  db.exec "UPDATE grid_squares SET views_per_capita = total_views/population::float WHERE population > 0"
end

desc "describe the largest city/town in each grid_square"
task :assign_grid_square_names do
  sql = <<-EOF
    SELECT gs.id, c.name
    FROM grid_squares gs,
      LATERAL (
        SELECT c.name || ', ' || c.state as name
          FROM cities c
        WHERE
          ST_Intersects(gs.the_geom, c.geom)
        ORDER BY
          c.pop_2010 DESC NULLS LAST
        LIMIT 1
      ) c
  EOF
  result = db.exec sql
  result.each do |row|
    db.exec_params("UPDATE grid_squares SET name = $1 WHERE id = $2::int", [row['name'], row['id']])
  end
end

# select
#   name, population, views_per_capita
# from grid_squares
# where
#   population > 100000
# order by
#   views_per_capita desc;

# TODO:
# stats on grid squares. histogram of populations.
#   can/should we use this to decide a min population for a square?
# exclude AWS Oregon IP addresses. import all of 2014.
# exclude non-US views in videometrics SQL.
# can we group by region? what's a region anyway?
# or report per state. (square 1 is state X, square 2 is state Y, etc. overall how do states rank?)

task default: [:calculate_views_per_capita, :assign_grid_square_names, :load_states]

