import numpy as np
import matplotlib.pyplot as plt

read_file = open('values', 'r')

value_list = []
for line in read_file:
    value_list.append(int(line.strip()))

x = [i+1 for i in range(16)]

print value_list
print x

plt.plot(x, value_list)
plt.title('Plot of operations/s vs number of nodes')
plt.xlabel('# of nodes')
plt.ylabel('operations per second')


plt.show()
