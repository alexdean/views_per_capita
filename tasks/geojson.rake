namespace :geojson do

  output_file = './out/views_per_capita.geojson'
  file output_file do
    # http://gis.stackexchange.com/questions/124413/how-to-export-thousands-of-features-from-postgis-to-a-single-geojson-file/124415#124415
    sql = <<-EOF
    SELECT row_to_json(featcoll)
    FROM (
      SELECT 'FeatureCollection' As type, array_to_json(array_agg(feat)) As features
      FROM (
        SELECT
          'Feature' As type,
          ST_AsGeoJSON(tbl.geom)::json As geometry,
          row_to_json(
            (SELECT l FROM (SELECT id, name, rank, percentile, population, total_views, views_per_person_per_week) As l)
          ) As properties
        FROM grid_squares As tbL
      ) as feat
    ) as featcoll;
    EOF

    result = db.exec sql
    File.open(output_file, 'w') {|f| f.write(result.first['row_to_json'])}
  end
  task generate: output_file
end

task geojson: "geojson:generate"
