require 'csv'
require 'pg'

def db_name
  'views_per_capita'
end

def db
  @db ||= PG.connect(dbname: db_name)
end


task :create_functions do
  # http://www.faqoverflow.com/gis/16374.html
  sql = <<-EOF
  CREATE OR REPLACE FUNCTION ST_CreateFishnet(
          nrow integer, ncol integer,
          xsize float8, ysize float8,
          x0 float8 DEFAULT 0, y0 float8 DEFAULT 0,
          OUT "row" integer, OUT col integer,
          OUT geom geometry)
      RETURNS SETOF record AS
  $$
  SELECT i + 1 AS row, j + 1 AS col, ST_Translate(cell, j * $3 + $5, i * $4 + $6) AS geom
  FROM generate_series(0, $1 - 1) AS i,
       generate_series(0, $2 - 1) AS j,
  (
  SELECT ('POLYGON((0 0, 0 '||$4||', '||$3||' '||$4||', '||$3||' 0,0 0))')::geometry AS cell
  ) AS foo;
  $$ LANGUAGE sql IMMUTABLE STRICT;
  EOF
  db.exec sql
end

task :create_tables do
  sql = <<-EOF
  CREATE TABLE IF NOT EXISTS locations (
    id integer primary key,
    latitude numeric (8,5),
    longitude numeric (8,5)
  );
  EOF
  db.exec sql
  db.exec "SELECT AddGeometryColumn('locations', 'point', 4326, 'POINT', 2, false)"
  db.exec "create index on locations using gist (point)"

  db.exec "CREATE TABLE IF NOT EXISTS cells (id serial primary key, population integer);"
  db.exec "SELECT AddGeometryColumn('cells', 'geom', 4326, 'POLYGON', 2);"
  db.exec "CREATE INDEX ON cells USING GIST (geom);"
end

task :build_grid do
  # build a grid that covers the entire area where we have locations
  # parameters determined manually.
  db.exec "delete from cells"
  db.exec "insert into cells (geom) select ST_SetSRID(geom, 4326) from ST_CreateFishnet(57, 115, 1, 1, -180, 15)"

  # remove cells which have no locations
  # should be able to do this with one query, but getting unexpected results
  # so opting for the dumb/procedural approach instead.
  # run select to see. (many (all?) cells join to a location with NULL id, which doesn't exist in locations table.)
  # delete from cells where id IN (select c.id from cells c left join locations l ON (ST_Contains(c.geom, l.point)) group by c.id having count(*) = 1);
  no_locations = []
  result = db.exec("select id from cells")
  result.each do |row|
    sql = <<-EOF
      SELECT count(*) as location_count
      FROM cells ce
        LEFT JOIN locations l ON ST_Contains(ce.geom, l.point)
      WHERE
        ce.id = #{row['id']}
        AND l.id IS NOT NULL
    EOF
    result = db.exec(sql)
    if result[0]['location_count'].to_i == 0
      no_locations << row['id']
    end
  end

  if no_locations.size > 0
    db.exec("DELETE FROM cells WHERE id IN (#{no_locations.join(',')})")
  end

  cell_count = db.exec("select count(*) as cell_count from cells")[0]['cell_count'].to_f

  result = db.exec("select id from cells")
  result.each_with_index do |row, idx|
    # find counties which overlap this cell.
    # calculate % of the county which falls within the cell.
    # apply that % of the county's population to the cell.
    # (assume the county's population is evenly distributed.)
    sql = <<-EOF
      SELECT
        co.name,
        co.total_population,
        ST_Area(ST_Intersection(ce.geom, co.geom)) as overlap_area,
        ST_Area(co.geom) as total_county_area
      FROM cells ce
        INNER JOIN counties co ON ST_Intersects(ce.geom, co.geom)
      WHERE
        ce.id = #{row['id']}
    EOF

    cell_population = 0
    result2 = db.exec(sql)
    result2.each do |county|
      pct_of_county_in_cell = county['overlap_area'].to_f / county['total_county_area'].to_f
      pct_of_county_population_in_cell = pct_of_county_in_cell * county['total_population'].to_i
      cell_population += pct_of_county_population_in_cell.to_i
    end

    db.exec "UPDATE cells SET population = #{cell_population} WHERE id = #{row['id']}"

    if idx > 0 && idx % 50 == 0
      print '.'

      if idx % 200 == 0
        print " #{(idx/cell_count*100).round}%"
        puts
      end
    end
  end

  puts
end

counties_base = 'County_2010Census_DP1'
counties_zip_file = "source_data/#{counties_base}.zip"
counties_source_url = 'http://www2.census.gov/geo/tiger/TIGER2010DP1/County_2010Census_DP1.zip'
task counties_zip_file do
  # `wget -O #{counties_zip_file} #{counties_source_url}`

  if ! File.exist?(counties_zip_file) || File.size(counties_zip_file) == 0
    puts "#{counties_zip_file} is missing, and downloading from #{counties_source_url} failed."
    puts "Try downloading a copy from the '2010 Census' > 'Counties' section of http://www.census.gov/geo/maps-data/data/tiger-data.html"
    puts
    puts "If this file is no longer available:"
    puts "  - fetch the closest equivalent"
    puts "  - save it (in zip format) into the source_data/ directory"
    puts "  - adjust the `counties_base` variable in this Rakefile"
    puts "  - inspect the data to see if the RENAME commands in the `load_counties` task need to be adjusted."
    ptus "    the zip file should contain a spreadsheet describing the actual columns in the dbf data file"
    exit
  end
end
task counties_zip_file: counties_zip_file

task load_counties: :counties_zip_file do
  db.exec "DROP TABLE IF EXISTS counties"
  `rm -Rf tmp/#{counties_base}`
  `unzip -o -d tmp/#{counties_base} #{counties_zip_file}`
  `shp2pgsql -s 4326 -t 2D -W LATIN1 -I source_data/County_2010Census_DP1/County_2010Census_DP1.shp counties > tmp/counties.sql`
  `psql #{db_name} < tmp/counties.sql`
  db.exec "ALTER TABLE counties RENAME COLUMN dp0010001 TO total_population"
  db.exec "ALTER TABLE counties RENAME COLUMN namelsad10 TO name"
end

task :load_locations do
  # from videometrics database
  # SELECT id, latitude, longitude FROM locations WHERE country_id = 10;
  # export to locations.csv
  csv_file = 'source_data/locations.csv'

  sql_file = 'tmp/locations.sql'

  out = File.open(sql_file, 'w')
  CSV.foreach(csv_file, headers: true) do |line|
    sql = <<-EOF
      INSERT INTO locations (id, latitude, longitude, point)
      VALUES (
        #{line['id']},
        #{line['latitude']},
        #{line['longitude']},
        ST_SetSRID(ST_Point(#{line['longitude']}, #{line['latitude']}), 4326));
    EOF
    out.write(sql.gsub(/\n */, " ").strip+"\n")
  end
  out.close
exit
  `psql #{db_name} < #{sql_file}`
end






