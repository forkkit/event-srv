FROM alpine:3.2
ADD event-srv /event-srv
ENTRYPOINT [ "/event-srv" ]
