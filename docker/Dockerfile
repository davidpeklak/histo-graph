# Use an official Python runtime as a parent image
# FROM debian:stretch
FROM rust:stretch

# Set the working directory to /app
WORKDIR /app

# Copy contents into the container at /app
# COPY refajo /app
COPY keepalive.sh /app

# Run app.py when the container launches
CMD ["./keepalive.sh"]
