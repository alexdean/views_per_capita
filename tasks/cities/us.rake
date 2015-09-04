namespace :cities do
  namespace :us do

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

    task load: cities_tarball do
      table_name = 'us_cities'
      if ! table_exists?(table_name)
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

        db.exec "DELETE FROM cities WHERE country = 'us'"
        db.exec "INSERT INTO cities (name, country, population, geom) SELECT name || ', ' || state, 'us', pop_2010, geom from #{table_name}"
      end
    end

  end
end
