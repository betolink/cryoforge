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
    '
     {"type": "number",
      "title": "Percent Valid Pixels",
      "maximum": 100,
      "minimum": 0,
      "description": "Percentage of valid glacier pixels"
     } 
    '
    NULL,
    'to_float', 
    'BTREE'
);
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
    'dt_days',
    '{"type": "number",
      "title": "Days of Separation",
      "maximum": 180,
      "minimum": 7,
      "description": "Days of separation between image pairs"
     }',
    NULL,
    'to_float',
    'BTREE'
);
