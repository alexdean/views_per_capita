Dir['./tasks/**/*.rake'].each do |file|
  load file
end

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

def root_dir
  @root_dir ||= File.dirname(File.realpath(__FILE__))
end

def table_exists?(table_name)
  result = db.exec "select count(*) as table_exists from pg_tables where tablename = '#{table_name}'"
  result[0]['table_exists'] == '0' ? false : true
end

def load_shp_file(table:, zip_file:nil, shp_file:nil, srid:4326)
  db.exec "DROP TABLE IF EXISTS #{table}"

  if zip_file
    ext = File.extname(zip_file)
    base = File.basename(zip_file, ext)
    tmp_dir = "tmp/#{base}"
    `rm -Rf #{tmp_dir}`
    `unzip -o -d #{tmp_dir} #{zip_file}`
    shp_file = "#{tmp_dir}/#{base}.shp"
  end

  if ! shp_file
    raise "zip_file or shp_file is required"
  end

  out_sql = "tmp/#{table}.sql"
  `shp2pgsql -s #{srid}:4326 -t 2D -W LATIN1 -g geom -I #{shp_file} #{table} > #{out_sql}`
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
          OUT geom geometry)
      RETURNS SETOF record AS
  $$
  SELECT i + 1 AS row, j + 1 AS col, ST_Translate(geom, j * $3 + $5, i * $4 + $6) AS geom
  FROM
    generate_series(0, $1 - 1) AS i,
    generate_series(0, $2 - 1) AS j,
    (
      SELECT ('POLYGON((0 0, 0 '||$4||', '||$3||' '||$4||', '||$3||' 0,0 0))')::geometry AS geom
    ) AS foo;
  $$ LANGUAGE sql IMMUTABLE STRICT;
  EOF
  db.exec sql
end

task default: [:setup, :grid, :geojson]
