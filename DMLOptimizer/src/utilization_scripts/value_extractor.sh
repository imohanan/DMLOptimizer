# memory
awk '{print $5}' dblab220_memory_1.txt > memfile1.txt
grep -oE '[0-9]+[.][0-9]+' memfile1.txt > memfile.txt
rm memfile1.txt
# CPU
awk '{print $9}' dblab220_CPU_1.txt > cpufile1.txt
grep -oE '[0-9]+[.][0-9]+' cpufile1.txt > cpufile.txt
rm cpufile1.txt
#Disk
awk '{printf("%s\t%s\n"), $3, $11}' dblab220_disk_1.txt > diskfile1.txt
grep -e '^dev' diskfile1.txt > diskfile.txt
rm diskfile1.txt
#Network
awk '{printf("%s\t%s\t%s\n"), $3, $6, $7}' dblab220_network_1.txt > network1.txt
awk '/[0-9]+.[0-9]+/' network1.txt > network.txt
rm network1.txt
