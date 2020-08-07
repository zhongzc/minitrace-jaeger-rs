# minitrace-jaeger-rs

```sh
$ docker run --rm -d -p6831:6831/udp -p6832:6832/udp -p16686:16686 jaegertracing/all-in-one:latest

$ cargo test

$ firefox http://localhost:16686/
```
