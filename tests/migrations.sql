UPDATE collections set partition_trunc='year' WHERE id='itslive-granules';
-- need to researh how to do this properly, with descriptions and types

INSERT INTO pgstac.queryables (
    collection_ids, 
    name, 
    definition, 
    property_path,
    property_wrapper, 
    property_index_type
)
VALUES (
    NULL, 
    'percent_valid_pixels',
    '{
        "type": "number",
        "title": "Percent Valid Pixels",
        "description": "Percentage of valid (non-cloudy, non-nodata) pixels in the item"
    }',
    'properties.percent_valid_pix',
    'to_float', 
    'BTREE'
);
