# run and build the image created from dockerfile
docker build .
docker build -t imagename .

# check for built images in dir
docker images 


# to run/execute the imaege
# (you'll see IMAGE ID when you run docker images)
docker run image_id_of_imagename  