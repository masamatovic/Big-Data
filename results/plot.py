import pandas as pd
import matplotlib.pyplot as plt

data = pd.read_csv('r2017/part-00000-351b7e9f-a416-45d8-a4c8-88c1d36ffacb-c000.csv', skip_blank_lines=True)

s = pd.DataFrame(data)
s1 = s.loc[s['city'] == 'Chennai']
del s1['city']
s1 = s1.sort_values(by=['aqi'])

s1.set_index('aqi', inplace=True)
s1.head()

plt.subplot(2, 2, 1)
plt.title('Chennai')
plt.bar(s1.index, s1['count'], tick_label=s1.index, color=['green', 'yellow', 'orange', 'red', 'purple', 'maroon'])

s2 = s.loc[s['city'] == 'Mumbai']
del s2['city']
s2 = s2.sort_values(by=['aqi'])
s2.set_index('aqi', inplace=True)
s2.head()

plt.subplot(2, 2, 2)
plt.title('Mumbai')
plt.bar(s2.index, s2['count'], tick_label=s2.index, color=['green', 'yellow', 'orange', 'red', 'purple', 'maroon'])

s3 = s.loc[s['city'] == 'Delhi']
del s3['city']
s3 = s3.sort_values(by=['aqi'])
s3.set_index('aqi', inplace=True)
s3.head()

plt.subplot(2, 2, 3)
plt.title('Delhi')
plt.bar(s3.index, s3['count'], tick_label=s3.index, color=['green', 'yellow', 'orange', 'red', 'purple', 'maroon'])

s4 = s.loc[s['city'] == 'Hyderabad']
del s4['city']
s4 = s4.sort_values(by=['aqi'])
s4.set_index('aqi', inplace=True)
s4.head()

plt.subplot(2, 2, 4)
plt.title('Hyderabad')
plt.bar(s4.index, s4['count'], tick_label=s4.index, color=['green', 'yellow', 'orange', 'red', 'purple', 'maroon'])

plt.tight_layout()
plt.show()