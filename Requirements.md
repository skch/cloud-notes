# Requirements and limitations

The following are restrictions dictated of SimpleDB:

* Database name: 3-255 characters (a-z, A-Z, 0-9, ‘_’, ‘-‘, and ‘.’)
* Items per document: 256
* Item name length: 1024 bytes
* Item text value length: 1024 bytes (anything larger than that is stored as an attachment)
* Document name length: 1024 bytes
* Maximum number of documents returned in search: 2500
* Maximum number of items per Select expression: 20
* Maximum number of comparisons per Select expression: 20
* Maximum response size for Select: 1MB

