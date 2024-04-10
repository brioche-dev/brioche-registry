#!/usr/bin/env bash

if [ ! -f "$LITEFS_CONFIG" ]; then
  echo "LiteFS config file not found: LITEFS_CONFIG=$LITEFS_CONFIG"
  exit 1
fi

litefs mount -config "$LITEFS_CONFIG"
