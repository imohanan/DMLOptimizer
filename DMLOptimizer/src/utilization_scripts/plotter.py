import os
import sys
import time
import matplotlib.pyplot as plt
xm = []
memlines = [line.rstrip('\n') for line in open('memfile.txt')]
for i in range(0,len(memlines)):
	xm.append(i*10)

#CPU
xc = []
cpulines = [line.rstrip('\n') for line in open('cpufile.txt')]
for i in range(0,len(cpulines)):
	xc.append(i*10)

#Disk
#disks = [line.rstrip('\n') for line in open('disks.txt')]
disk_map = dict()
with open('diskfile.txt') as f:
    for line in f:
    	words = line.split()
    	if words[0] not in disk_map:
    		disk_map.setdefault(words[0], [])
    	disk_map[words[0]].append(words[1])
xd = []
for i in range(0,len(disk_map.values()[0])):
	xd.append(i*10)

#Network
xn=[]
net_rec = dict()
net_trans = dict()
#netcard = [line.rstrip('\n') for line in open('networkcard.txt')]
with open('network.txt') as f:
	for line in f:
		words = line.split()
		if words[0] not in net_trans:
			net_trans.setdefault(words[0],[])
			net_rec.setdefault(words[0],[])
		net_rec[words[0]].append(words[1])
		net_trans[words[0]].append(words[2])
for i in range(0,len(net_trans.values()[0])):
	xn.append(i*10)

timestr = time.strftime("%Y%m%d-%H%M%S")
print xm
#Memory plot
plt.figure()
plt.suptitle('Memory Utilization')
plt.plot(xm,memlines,linewidth=2)
plt.ylabel('% memory used')
plt.xlabel('Time in seconds')
plt.savefig('/'+timestr+'/memory_utilization_'+timestr+'.pdf')
#plt.show()

#CPU plot
plt.figure()
plt.suptitle('CPU Utilization')
plt.plot(xc,cpulines,linewidth=2)
plt.ylabel('% idle CPU')
plt.xlabel('Time in seconds')
plt.savefig('/'+timestr+'/cpu_utilization_'+timestr+'.pdf')

#Disk Plot
plt.figure()
plt.gca().set_color_cycle(['red', 'blue', 'green', 'yellow'])
plt.suptitle('Disk Utilization')
plt.ylabel('% Disk utilization')
plt.xlabel('Time in seconds')
for i in range (0,len(disk_map.values())):
	plt.plot(xd,disk_map.values()[i],linewidth=2)
plt.savefig('/'+timestr+'/disk_utilization_'+timestr+'.pdf')

#Network Plot
plt.figure()
plt.gca().set_color_cycle(['red', 'blue', 'green', 'yellow'])
plt.suptitle('Network Utilization')
plt.ylabel('rxkb/s')
plt.xlabel('Time in seconds')
for i in range (0,len(net_rec.values())):
	plt.plot(xn,net_rec.values()[i],linewidth=2)
plt.savefig('/'+timestr+'/network_utilization_recieved_kb_'+timestr+'.pdf')

plt.figure()
plt.gca().set_color_cycle(['red', 'blue', 'green', 'yellow'])
plt.suptitle('Network Utilization')
plt.ylabel('txkb/s')
plt.xlabel('Time in seconds')
for i in range (0,len(net_trans.values())):
	plt.plot(xn,net_trans.values()[i],linewidth=2)
plt.savefig('/'+timestr+'/network_utilization_transmit_kb_'+timestr+'.pdf')




