#!/usr/bin/env bash 

godoc -goroot="." -http=:6060 &> http.log &
pid=$!

echo "web server started with pid $pid"

sleep 5

echo "Downloading files..."
wget --no-host-directories  --directory-prefix godoc  -r -np -N -E -p -k http://localhost:6060/pkg/ &> gen.log || true

echo "Killing http server..."

kill $pid

echo "wget output is:"
cat gen.log

rm gen.log
rm http.log
