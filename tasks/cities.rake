namespace :cities do

  desc 'create unified table of city locations'
  task :create do
    if ! table_exists?('cities')
      sql = <<-EOF
      CREATE TABLE cities (
        id serial primary key,
        country char(2),
        name varchar(255),
        population integer
      );
      EOF
      db.exec sql
      db.exec "SELECT AddGeometryColumn('cities', 'geom', #{srid}, 'POINT', 2, false)"
      db.exec "create index on cities using gist (geom)"
    end
    # id, name ("name, state"), population, geom
  end

  desc 'load all city data'
  task load: [:create, "cities:us:load", "cities:ca:load"]

end

task cities: "cities:load"
