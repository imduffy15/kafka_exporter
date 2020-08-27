FROM        debian:slim

COPY kafka_exporter /bin/kafka_exporter

EXPOSE     9308
ENTRYPOINT [ "/bin/kafka_exporter" ]