CREATE CATALOG {{catalog_name}} WITH (
    'type'                               = 'iceberg',
    'catalog-type'                       = 'rest',
    'uri'                                = {{polaris_uri}},
    'credential'                         = {{credential}},
    'warehouse'                          = {{warehouse}},
    'scope'                              = 'PRINCIPAL_ROLE:ALL',
    'header.Polaris-Realm'               = {{realm}},
    'header.X-Iceberg-Access-Delegation' = 'vended-credentials',
    'io-impl'                            = 'org.apache.iceberg.aws.s3.S3FileIO'
)
