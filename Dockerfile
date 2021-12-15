FROM golang:1.16-alpine as builder
RUN apk add --no-cache git 
WORKDIR /go/pgstat2ilp
COPY . .
RUN go generate -x -v
RUN go build -v
 
FROM alpine:latest 
ENV DB_URL "postgres://postgres:password@localhost:5432/postgres?sslmode=disable"
ENV INFLUX_URL "localhost:9009"
COPY --from=builder /go/pgstat2ilp/pgstat2ilp /pgstat2ilp
ENTRYPOINT /pgstat2ilp -dsn $DB_URL -influx $INFLUX_URL