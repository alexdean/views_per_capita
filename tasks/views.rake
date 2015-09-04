def view_days
  # WHERE happened_at BETWEEN '2014-01-01 08:00:00' AND '2015-06-01 07:59:59'
  516
end

namespace :views do

  task :create_table do
    if ! table_exists?('view_locations')
      sql = <<-EOF
      CREATE TABLE view_locations (
        id integer primary key,
        total_views integer
      );
      EOF
      db.exec sql
      db.exec "SELECT AddGeometryColumn('view_locations', 'geom', #{srid}, 'POINT', 2, false)"
      db.exec "create index on view_locations using gist (geom)"
    end
  end

  task load: :create_table do

    count = db.exec("SELECT COUNT(*) as the_count from view_locations")[0]['the_count']
    if count == 0


      # from videometrics database
      # SELECT id, latitude, longitude FROM locations WHERE country_id IN (1,10);
      # export to view_locations.csv
      location_csv = 'source_data/view_locations.csv'
      # SELECT location_id, count(*) as total_views FROM events WHERE happened_at BETWEEN '2014-01-01 08:00:00' AND '2015-06-01 07:59:59' GROUP BY location_id;
      views_per_location_csv = 'source_data/views_per_location.csv'

      views_per_location = {}
      CSV.foreach(views_per_location_csv, headers: true) do |line|
        views_per_location[line['location_id'].to_i] = line['total_views']
      end

      # write a postgres/postgis sql file from the videometrics csv exports
      sql_file = 'tmp/view_locations.sql'
      out = File.open(sql_file, 'w')
      CSV.foreach(location_csv, headers: true) do |line|
        sql = <<-EOF
          INSERT INTO view_locations (id, total_views, geom)
          VALUES (
            #{line['id']},
            #{views_per_location[line['id'].to_i].to_i},
            ST_SetSRID(ST_Point(#{line['longitude']}, #{line['latitude']}), #{srid}));
        EOF
        out.write(sql.gsub(/\n */, " ").strip+"\n")
      end
      out.close

      # NOTE: we can end up with some locations having 0 views. The location could
      # exist because it received views outside of the time range we requested on
      # the `events` table.

      `psql #{db_name} < #{sql_file}`
    end
  end

end

task views: "views:load"
