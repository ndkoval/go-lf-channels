import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter, StrMethodFormatter, LogFormatterExponent, LogFormatterMathtext, \
    FormatStrFormatter
import os
import itertools
import statistics

from pip._vendor.msgpack.fallback import xrange

colors = ["#66a61e", "#d95f02", "#e6ab02", "#7570b3"]
markers = ['>', 'x', 's', 'd', "D", 'x', '2']

# + color pair (#e78ac3, #e7298a) + "#7570b3", "#8da0cb"

f = open("results.txt", "r")
lines = f.readlines()
results = []
for l in lines:
    if not l.startswith("Benchmark"): continue
    configuration = {}
    configuration['benchmark'] = l[:l.find('/')]
    l = l[(l.find('/')+1):]
    configurationStr = l.split()[0].split('#')[0].split('-')[0]
    for p in configurationStr.split('/'):
        key = p.split('=')[0]
        value = p.split('=')[1]
        configuration[key] = value
    configuration['result'] = float(l.split()[2])
    # configuration['result'] = 1000000 / float(l.split()[2])
    results.append(configuration)

threads = []
benchmarksAndGoroutines = []
works = []

for r in results:
    if r['threads'] not in threads: threads.append(r['threads'])
    if [r['benchmark'],r['goroutines']] not in benchmarksAndGoroutines: benchmarksAndGoroutines.append([r['benchmark'],r['goroutines']])
    if r['work'] not in works: works.append(r['work'])

print(threads)
print(benchmarksAndGoroutines)
print(works)

def draw(ax, benchmark, goroutines, work):
    # ax.set_xscale('log', basex=2)
    # ax.xaxis.set_major_formatter(FormatStrFormatter('%0.f'))
    ax.grid(linewidth='0.5', color='lightgray')
    benchmark = bag[0]
    goroutines = bag[1]
    ax.set_title(benchmark + ", #goroutines = " + ('#threads' if goroutines == 0 else goroutines) + ', work = ' + work, size=8)

    markerAndColorIndex = 0
    for newAlgo in ['true', 'false']:
        for withSelect in ['true', 'false']:
            label = 'algo=' + ('new' if newAlgo=='true' else 'old') + ',select=' + withSelect
            curResults = []
            for t in threads:
                threadResults = []
                for r in results:
                    if r['benchmark'] == benchmark and r['goroutines'] == goroutines and r['work'] == work and r['newAlgo'] == newAlgo and r['withSelect'] == withSelect and r['threads'] == t:
                        threadResults.append(r['result'])
                curResults.append(statistics.mean(threadResults))
            ax.plot(threads,
                    curResults,
                    label=label,
                    marker=markers[markerAndColorIndex % len(markers)],
                    color=colors[markerAndColorIndex % len(colors)])
            markerAndColorIndex += 1

    ax.set_ylim(ymin=0)
    ax.set_ylabel('ns / op')
    ax.set_xlabel('Number of scheduler threads')


plots = len(benchmarksAndGoroutines) * len(works)
f, axarr = plt.subplots(plots, 1, figsize=(10, plots * 6))

for i, bag in enumerate(benchmarksAndGoroutines):
    for j, w in enumerate(works):
        plot = i * len(works) + j
        draw(axarr[plot], bag[0], bag[1], w)

plt.tight_layout(pad=2, w_pad=2, h_pad=1)
lines, labels = axarr[0].get_legend_handles_labels()
f.legend(lines, labels, loc='upper center', borderpad=0, ncol=4, frameon=False, prop={'size': 8})
# plt.show()
f.savefig("results.pdf", bbox_inches='tight')