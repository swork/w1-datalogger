* w1-datalogger

Walks the /sys/bus tree of recent Linux kernels for 1Wire devices, polls each,
and POSTs a JSON blob of the results to a configured endpoint. I use it to
record info supporting my house heating system, but this code imposes no
semantics on the data it collects.

 - Steve Work, January 2020
