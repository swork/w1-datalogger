* w1-datalogger

Walks the /sys/bus tree of recent Linux kernels for 1Wire devices, polls each,
and POSTs a JSON blob of the results to a configured endpoint. I use it to
record info supporting my house heating system, but this code imposes no
semantics on the data it collects.

** Work TBD **

There's no error handling. On fail to post it needs to tuck the observations
away somewhere; on each run it needs to retrieve them and include in the next
post (idempotently, atomically, and safe against concurrency).

There's no local logging. It should have the option of recording its activity.

The module name 'logger' is a really poor choice, conflicting with common use of
Python logging facility and way too broad. Should stick with w1logger or
similar.

 - Steve Work, January 2020
