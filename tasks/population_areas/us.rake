namespace :population_areas do
  namespace :us do

    population_updates_base = 'CO-EST2014-alldata'
    population_updates_file = "source_data/#{population_updates_base}.csv"
    population_updates_source_url = "http://www.census.gov/popest/data/counties/totals/2014/files/#{population_updates_base}.csv"
    # desc "fetch population updates 2010-2014"
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
    # desc "fetch county definitions from Census Bureau website"
    task counties_zip_file: counties_zip_file

    desc "put US county data into unified population_areas table"
    task load: ["population_areas:create", counties_zip_file] do

      if ! table_exists?('us_counties')

        load_shp_file(table: 'us_counties', zip_file: counties_zip_file)

        # db.exec "ALTER TABLE counties RENAME COLUMN dp0010001 TO total_population"
        # db.exec "ALTER TABLE counties RENAME COLUMN namelsad10 TO name"
        db.exec "ALTER TABLE us_counties ADD pop_est_2010 integer, ADD pop_est_2014 integer"

        CSV.foreach(population_updates_file, encoding: "iso-8859-1:UTF-8", headers: true) do |line|
          fips = line['STATE'] + line['COUNTY']
          sql = <<-EOF
            UPDATE us_counties
            SET
              pop_est_2010 = #{line['POPESTIMATE2010']},
              pop_est_2014 = #{line['POPESTIMATE2014']}
            WHERE geoid10 = '#{fips}'
          EOF
          db.exec sql
        end

      end

      db.exec "DELETE FROM population_areas WHERE country = 'us'"
      sql = <<-EOF
        INSERT INTO population_areas (country, population, population_year, name, geom)
        SELECT
          'us',
          pop_est_2010,
          2010,
          namelsad10,
          geom
        FROM us_counties
      EOF
      db.exec sql

    end

  end
end
