import datetime
import logging
import pprint

import numpy as np
import yaml

import data_aggregator

logging.basicConfig(level=logging.DEBUG)
kibana_config = yaml.safe_load(open('kibana.yaml'))['kibana']
crawler = data_aggregator.DataCrawler(kibana_config['url'], kibana_config['username'], kibana_config['password'])

#job = crawler.getSlurmJob(data_aggregator.Cluster.Daint, 718948, fillNodeXname=True)
job = crawler.getSlurmJob(data_aggregator.Cluster.Clariden, 347469, fillNodeXname=True)
#pprint.pprint(job)

start = datetime.datetime.now()
gpu_temps = crawler.getGpuTemperature([job.nodes[0]], job.start, job.end)
print(f'It took {(datetime.datetime.now()-start).total_seconds()} seconds to query GPU temperature data')
for gpu_idx in range(0, 4):
    temp_data = gpu_temps.getTemperatureData(job.nodes[0].nid, gpu_idx)
    #for idx in range(len(temp_data[0])-1):
    #    print((temp_data[0][idx+1]-temp_data[0][idx]).total_seconds())
    #print(temp_data[1])
    print(len(temp_data[0]),  (job.end-job.start).total_seconds(), max(temp_data[1]))
