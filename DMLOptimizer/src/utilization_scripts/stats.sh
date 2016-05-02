killall sar
rm -r dblab*
sar -u 10 >> dblab220_CPU_1.txt &
sar -n DEV 10 >> dblab220_network_1.txt &
sar -r 10 >> dblab220_memory_1.txt &
sar -d 10 >> dblab220_disk_1.txt &
