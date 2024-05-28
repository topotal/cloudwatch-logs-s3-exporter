FROM golang:1.21 as build
WORKDIR /cloudwatch-logs-s3-exporter
COPY go.mod go.sum ./
COPY main.go .
COPY internal internal
RUN go build -tags lambda.norpc -o main main.go

FROM public.ecr.aws/lambda/provided:al2023
COPY --from=build /cloudwatch-logs-s3-exporter/main /var/task/bootstrap
ENTRYPOINT [ "./var/task/bootstrap" ]
