namespace :cities do
  namespace :ca do

    # source_file_tarball = "source_data/pop000x_shp_nt00304.tar.gz"
    # source_file_url = "http://dds.cr.usgs.gov/pub/data/nationalatlas/#{source_file_tarball}"
    populated_places_base = "pop000x_shp_nt00304"
    populated_places_tarball = "source_data/#{populated_places_base}.tar.gz"
    populated_places_shpfile_base = "pop_pnt"
    populated_places_source_url = "http://dds.cr.usgs.gov/pub/data/nationalatlas/#{populated_places_base}.tar.gz"
    file populated_places_tarball do
      `wget -O #{populated_places_tarball} #{populated_places_source_url}`
      if ! File.exist?(populated_places_tarball) || File.size(populated_places_tarball) == 0
        puts "#{populated_places_tarball} is missing. Downloading from #{populated_places_source_url} failed."
        puts "Try visiting http://catalog.data.gov/dataset/usgs-small-scale-dataset-north-american-atlas-populated-places-200406-shapefile to find a replacement."
        exit 1
      end
    end

    task load: populated_places_tarball do
      table_name = "ca_cities"
      if ! table_exists?(table_name)
        tmp_dir = "tmp/#{populated_places_base}"
        `mkdir #{tmp_dir}`
        `cp #{populated_places_tarball} #{tmp_dir}`
        `cd #{tmp_dir} && tar xzf #{populated_places_base}.tar.gz`
        load_shp_file(shp_file: "#{tmp_dir}/#{populated_places_shpfile_base}.shp", table: table_name, srid: 4269)

        db.exec "DELETE FROM #{table_name} WHERE country != 'CAN'"
        db.exec "ALTER TABLE #{table_name} ADD name_w_province varchar(255)"
        db.exec "UPDATE #{table_name} SET name_w_province = name || ', ' || substring(stateabb, 4)"

        # TODO: find a data set that includes populations.
        db.exec "DELETE FROM cities WHERE country = 'ca'"
        db.exec "INSERT INTO cities (name, country, population, geom) SELECT name_w_province, 'ca', popclass, geom from #{table_name}"
      end
    end

  end
end
