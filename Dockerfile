FROM golang:1.17

# Create a directory for the app
WORKDIR /app

# Copy the go.mod and go.sum files to the container
COPY go.mod go.sum ./

# Download the Go modules
RUN go mod download

# Copy the rest of the application files to the container
COPY . .

# Build the application
RUN go build -o pulsarETL ./cmd/pulsarETL

# Copy the configuration file
COPY ./config ./config

# Run the application
CMD ["./pulsarETL"]