namespace :population_areas do
  namespace :ca do

    ### WTF WTF WTF
    # TODO:  select sum(population) from population_areas where country= 'ca';
    # comes in at 40,782,321. online sources say the population of Canada is
    # 33 million. https://en.wikipedia.org/wiki/Demographics_of_Canada#Population
    namespace :census_subdivisions do
      ca_census_subdivisions_base = "gcsd000b11a_e"
      ca_census_subdivisions_zip_file = "source_data/#{ca_census_subdivisions_base}.zip"
      ca_census_subdivisions_source_url = "http://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/#{ca_census_subdivisions_base}.zip"
      file ca_census_subdivisions_zip_file do
        `wget -O #{ca_census_subdivisions_zip_file} #{ca_census_subdivisions_source_url}`

        if ! File.exist?(ca_census_subdivisions_zip_file) || File.size(ca_census_subdivisions_zip_file) == 0
          puts "Unable to download #{ca_census_subdivisions_zip_file} from #{ca_census_subdivisions_source_url}"
          puts "Look for a replacement on http://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/bound-limit-2011-eng.cfm"
          fail
        end
      end

      ca_census_subdivisions_population_base = '98-316-XWE2011001-301_CSV'
      ca_census_subdivisions_population_zip = "#{ca_census_subdivisions_population_base}.zip"
      ca_census_subdivisions_population_file = "source_data/#{ca_census_subdivisions_population_zip}"
      ca_census_subdivisions_population_url = "http://www12.statcan.gc.ca/census-recensement/2011/dp-pd/prof/details/download-telecharger/comprehensive/comp-csv-tab-dwnld-tlchrgr.cfm?Lang=E#tabs2011"
      ca_census_subdivisions_population_tmp_dir = "tmp/#{ca_census_subdivisions_population_base}"
      task :get_locations do

        if ! File.exist?(ca_census_subdivisions_population_file)
          puts "go to #{ca_census_subdivisions_population_url}"
          puts "select 'Census divisions' and 'CSV'"
          puts "download 98-316-XWE2011001-301_CSV.zip and save in source_data/"
          fail
        end

        tmp_dir = ca_census_subdivisions_population_tmp_dir
        `rm -Rf #{tmp_dir} && mkdir #{tmp_dir}`
        `unzip -o -d #{tmp_dir} #{ca_census_subdivisions_population_file}`
      end

      task load: [:get_locations, ca_census_subdivisions_zip_file] do

        if ! table_exists?('ca_census_subdivisions')
          # srid looked up by pasting contents of gcsd000b11a_e.prj into http://prj2epsg.org/search
          load_shp_file(table: 'ca_census_subdivisions', zip_file: ca_census_subdivisions_zip_file, srid: 4269)

          db.exec "ALTER TABLE ca_census_subdivisions ADD population_2011 integer"

          tmp_dir = ca_census_subdivisions_population_tmp_dir
          subset_file = "#{tmp_dir}/total_populations.csv"
          `grep 'Population in 2011' #{tmp_dir}/98-316-XWE2011001-301.CSV > #{subset_file}`

          # "Geo_Code" in CSV matches ccsuid in shp data.
          # Geo_Code,Prov_Name,CD_Name,CSD_Name,CSD_Type,Topic,Characteristics,Note,Total,Flag_Total,Male,Flag_Male,Female,Flag_Female
          CSV.foreach(subset_file) do |line|
            geo_code = line[0]
            total = line[8] || 'NULL'

            db.exec "UPDATE ca_census_subdivisions SET population_2011 = #{total} WHERE ccsuid = '#{geo_code}'"
          end
        end

        db.exec "DELETE FROM population_areas WHERE country = 'ca'"
        sql = <<-EOF
          INSERT INTO population_areas (country, population, population_year, name, geom)
          SELECT
            'ca',
            population_2011,
            2011,
            ccsname,
            geom
          FROM ca_census_subdivisions
          WHERE population_2011 IS NOT NULL
        EOF
        db.exec sql

      end

    end

    task load: "census_subdivisions:load"

  end
end
