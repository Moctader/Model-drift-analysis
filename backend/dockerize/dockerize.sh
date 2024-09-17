docker build . -t backend_api -f api.Dockerfile
docker run -p 3003:3003 backend_api