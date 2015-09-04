namespace :grid do

  desc 'create grid'
  task create: "views:load" do
    if ! table_exists?('grid_squares')
      sql = <<-EOF
        CREATE TABLE grid_squares (
          id serial primary key,
          name varchar(255),
          population integer,
          total_views integer,
          predominant_country char(2),
          rank integer,
          percentile numeric(4,3),
          views_per_person_per_week numeric (8,7)
        )
      EOF
      db.exec sql

      db.exec "SELECT AddGeometryColumn('grid_squares', 'geom', #{srid}, 'POLYGON', 2);"
      db.exec "CREATE INDEX ON grid_squares USING GIST (geom);"

      # build a grid that covers the entire area where we have view_locations
      # parameters determined manually.
      db.exec "delete from grid_squares"
      db.exec "insert into grid_squares (geom) select ST_SetSRID(geom, #{srid}) from ST_CreateFishnet(68, 128, 1, 1, -180, 15)"

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
            LEFT JOIN view_locations l ON ST_Contains(gs.geom, l.geom)
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
    end
  end

  desc 'set # of views'
  task set_views: [:create, "views:load"] do

    count = db.exec("SELECT COUNT(*) as the_count FROM grid_squares WHERE total_views IS NOT NULL")[0]['the_count'].to_i
    if count == 0
      sql = <<-EOF
        UPDATE grid_squares c
        SET total_views = x.total_views
        FROM (
          SELECT
            gs.id,
            SUM(l.total_views) as total_views
          FROM grid_squares gs
            INNER JOIN view_locations l ON ST_Contains(gs.geom, l.geom)
          GROUP BY
            gs.id
        ) x
        WHERE c.id = x.id
      EOF
      db.exec sql
    end

  end

  desc 'set populations'
  task set_populations: [:create, "population_areas:load"] do

    count = db.exec("SELECT COUNT(*) as the_count FROM grid_squares WHERE population IS NOT NULL")[0]['the_count'].to_i
    if count == 0
      grid_square_count = db.exec("select count(*) as grid_square_count from grid_squares")[0]['grid_square_count'].to_f

      result = db.exec("select id from grid_squares")
      result.each_with_index do |row, idx|
        # find population areas which overlap this grid_square.
        # calculate % of the area which falls within the grid_square.
        # apply that % of the area's population to the grid_square.
        # (assume the area's population is evenly distributed.)
        sql = <<-EOF
          SELECT
            pa.name,
            pa.population,
            pa.country,
            ST_Area(ST_Intersection(gs.geom, pa.geom)) as overlap_area,
            ST_Area(pa.geom) as total_area
          FROM grid_squares gs
            INNER JOIN population_areas pa ON ST_Intersects(gs.geom, pa.geom)
          WHERE
            gs.id = #{row['id']}
        EOF

        grid_square_population = 0
        result2 = db.exec(sql)
        country_pct = {}
        result2.each do |area|
          pct_of_area_in_grid_square = area['overlap_area'].to_f / area['total_area'].to_f
          area_population_in_grid_square = pct_of_area_in_grid_square * area['population'].to_i
          grid_square_population += area_population_in_grid_square.to_i

          # figure out what % of the square's population is in each country
          # so we can select the right set of cities to select a name from.
          country_pct[area['country']] ||= 0
          country_pct[area['country']] += area_population_in_grid_square.to_i
        end

        predominant_country = ''
        if country_pct.size > 0
          predominant_country = country_pct.sort_by {|key,val| -val}[0][0]
        end

        sql = "UPDATE grid_squares SET population = #{grid_square_population}, predominant_country = '#{predominant_country}' WHERE id = #{row['id']}"
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
  end

  task :assign_views_per_capita do #: [:set_views, :set_populations] do
    db.exec "UPDATE grid_squares SET views_per_person_per_week = total_views/#{(view_days/7).round}/population::float WHERE population > 0"
    # db.exec <<-EOF
    #   UPDATE grid_squares gs
    #   SET gs.rank = x.rank
    #   FROM (
    #       SELECT id, row_number() over(order by views_per_person_per_week desc nulls last)
    #       FROM grid_squares
    #     ) x
    #   WHERE
    #     gs.id = x.id
    # EOF

    count = db.exec("SELECT COUNT(*) as the_count FROM grid_squares")[0]['the_count'].to_i

    sql = <<-EOF
      SELECT id
      FROM grid_squares
      ORDER BY views_per_person_per_week DESC NULLS LAST
    EOF
    result = db.exec sql
    result.each_with_index do |row,idx|
      sql = <<-EOF
        UPDATE grid_squares
          SET
            rank = #{idx+1},
            percentile = #{(count-idx)/count.to_f}
        WHERE
          id = #{row['id']}
      EOF
      puts sql
      db.exec sql
    end

  end

  desc 'name squares based on city names'
  task assign_names: [:create, "cities:load"] do
    sql = <<-EOF
      SELECT gs.id, c.name
      FROM grid_squares gs,
        LATERAL (
          SELECT name
            FROM cities c
          WHERE
            ST_Intersects(gs.geom, c.geom)
            AND gs.predominant_country = c.country
          ORDER BY
            c.population DESC NULLS LAST
          LIMIT 1
        ) c
    EOF
    result = db.exec sql
    result.each do |row|
      db.exec_params("UPDATE grid_squares SET name = $1 WHERE id = $2::int", [row['name'], row['id']])
    end

  end

  task default: [:assign_names, :assign_views_per_capita]

end

task grid: "grid:default"
