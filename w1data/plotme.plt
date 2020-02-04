set xdata time
set timefmt "%s"
set format x "%m/%d/%y"
set key autotitle columnheader
plot "28-011912588b87.data" using 'time':2 with lines
