set xdata time
set timefmt "%s"
set format x "%m/%d/%y"
set key autotitle columnheader
plot "2020-02-office_air_temperature.data" using 'time':2 with lines
