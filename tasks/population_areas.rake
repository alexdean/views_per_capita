namespace :population_areas do

  # desc 'create unified table of population data'
  task :create do
    if ! table_exists?('population_areas')
      sql = <<-EOF
        CREATE TABLE population_areas (
          id serial primary key,
          country char(2),
          name varchar(255),
          population integer,
          population_year integer
        )
      EOF
      db.exec sql

      db.exec "SELECT AddGeometryColumn('population_areas', 'geom', #{srid}, 'MULTIPOLYGON', 2);"
      db.exec "CREATE INDEX ON population_areas USING GIST (geom);"
    end
  end

  desc 'load all population area data'
  task load: [:create, "population_areas:us:load", "population_areas:ca:load"]

end

task population_areas: "population_areas:load"
