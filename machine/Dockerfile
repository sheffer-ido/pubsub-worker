# Dockerfile References: https://docs.docker.com/engine/reference/builder/

# Start from the latest golang base image
FROM golang:1.14.6

# Add Maintainer Info
LABEL maintainer="Ido Sheffer"

# Set the Current Working Directory inside the container
WORKDIR /app


# Copy the source from the current directory to the Working Directory inside the container
COPY . .

RUN rm -rf /app/credentials

# Build the Go app
RUN go build -o main .


EXPOSE 9001

# Command to run the executable
CMD ["./main"]
