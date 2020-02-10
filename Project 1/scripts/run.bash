#!/bin/bash

python create_tables.py;
python etl2.py;

echo "script execution complete"