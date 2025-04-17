from dataclasses import dataclass
import datetime
from enum import Enum
import logging
from typing import Any, cast, Dict, List, Optional, Tuple, TypedDict

import requests

logger = logging.getLogger(__name__)

class ElasticHit(TypedDict):
    _index: str
    _id: str
    _score: float
    _ignored: List[str]
    _source: Dict[str, Any]

class Cluster(Enum):
    # completely randomized naming scheme for data in Elastic
    Daint    = 'alps-daint'
    Clariden = 'clariden'
    Santis   = 'alps-santis'
    Eiger    = 'eiger'

@dataclass
class Node:
    nid: str
    fullxname: str = ''
    # technically the below are all integer valued, but to avoid leading zeros being chopped off
    # we keep it a str
    cabinet: str = ''
    chassis: str = ''
    blade: str = ''
    bmc: str = ''
    node: str = ''




@dataclass
class SlurmJob:
    jobid: int
    name: str
    cluster: Cluster
    nodes: List[Node]
    #time data
    start: datetime.datetime
    end: datetime.datetime
    queued_sec: float
    runtime_sec: float
    # who submitted the job for which account
    username: str
    account: str
    # job exit status and state (note that exitcode can be 0 but state e.g. TIMEOUT)
    exit_code: int
    state: str
    # submission script
    script: str
    # where are the log files
    stdout_path: str
    stderr_path: str
    # full reply, has all data again and more
    full_reply: Dict[str, Any]


class GPUTemeraturepData(object):
    def __init__(self, data: List[ElasticHit]):
        self._temperature_data: Dict[str, List[Tuple[List[datetime.datetime], List[float]]]] = {}
        tmp_data: Dict[str, Dict[int, List[Tuple[datetime.datetime, float]]]] = {}
        for d in data:
            source = d['_source']
            nid = 'nid' + ('000000'+source['nid'])[-6:]
            temperature = source['Sensor']['Value']
            time = _to_local_datetime(source['@timestamp'])
            gpu_idx = source['Sensor']['Index']
            tmp_data.setdefault(nid, {}).setdefault(gpu_idx, []).append((time, temperature))
        for nid, temperatureByGpu in tmp_data.items():
            counter = 0
            self._temperature_data[nid] = []
            for gpu_idx in sorted(temperatureByGpu.keys()):
                assert gpu_idx == counter, f'{gpu_idx=} does not match {counter=}'
                counter += 1
                sorted_data = sorted(temperatureByGpu[gpu_idx], key=lambda dtval: dtval[0])
                self._temperature_data[nid].append(([ x[0] for x in sorted_data ], [ x[1] for x in sorted_data ] ))

    def getTemperatureData(self, nid: str, gpu_index: int) -> Tuple[List[datetime.datetime], List[float]]:
        return self._temperature_data[nid][gpu_index]


class DataCrawler(object):
    def __init__(self, url: str, username: str, password: str) -> None:
        self._base_url = url
        self._session = requests.session()
        self._session.auth = (username, password)

    def getSlurmJob(self, cluster: Cluster, jobid: int, fillNodeXname: bool=False) -> SlurmJob:
        query = {
            "bool": {
                "must": [
                    {"term":{"jobid":jobid}},
                    {"term":{"cluster":cluster.value}},
                ],
                "filter": [],
            },
        }
        all_results = self._crawl('.ds-logs-slurm.accounting-*', query)

        if len(all_results) > 1:
            logger.warn(f"Found more than one job matching {jobid=} for {cluster=}. Only first one will be returned")
        full_job = all_results[0]['_source']

        return SlurmJob(
            jobid=jobid,
            name=full_job['job_name'],
            cluster=cluster,
            nodes=self._toNodeObject(expand_nodes(full_job['nodes']), _to_local_datetime(full_job['@start']), fillNodeXname),
            start=_to_local_datetime(full_job['@start']),
            end=_to_local_datetime(full_job['@end']),
            queued_sec=full_job['@queue_wait'],
            runtime_sec=full_job['elapsed'],
            username=full_job['username'],
            account=full_job['account'],
            exit_code=full_job['exit_code'],
            state=full_job['state'],
            script=full_job['script'],
            stdout_path=full_job.get('std_out', ''),
            stderr_path=full_job.get('std_err', ''),
            full_reply=full_job,
        )

    def _toNodeObject(self, nodes: List[str], from_dt: datetime.datetime, fillNodeXname: bool) -> List[Node]:
        ret = [Node(nid=x) for x in nodes]
        if fillNodeXname:
            query = {
                "bool": {
                    "must":[
                        {"terms":{"nid": [x.replace('nid','').lstrip('0') for x in nodes]}},
                        {"match":{"Sensor.PhysicalContext": "GPU"}},
                        {"match":{"Sensor.Index": 0}},
                        {"match":{"MessageId":"CrayTelemetry.Temperature"}},
                    ],
                    "filter": [{
                        "range": {
                            "@timestamp": {
                                "format": "epoch_second",
                                "gte": int(from_dt.timestamp()),
                                "lt": int(from_dt.timestamp()+90),
                            }
                        }
                    }],
                }
            }
            data = self._crawl('.ds-metrics-facility.telemetry-alps*', query, limit=10000)
            dataByNid = { x['_source']['nid']: x['_source'] for x in data }
            for node in ret:
                nidKey = node.nid.replace('nid', '').lstrip('0')
                if nidKey not in dataByNid:
                    logging.warning(f'Could not find xname for {node.nid}')
                else:
                    locationData = dataByNid[nidKey]['Sensor']['LocationDetail']
                    node.fullxname = locationData['XName']
                    node.cabinet = locationData['Cabinet'].lstrip('x')
                    node.chassis = locationData['Chassie'].lstrip('c')
                    node.blade = locationData['Blade'].lstrip('s')
                    node.bmc = locationData['Bmc'].lstrip('b')
                    node.node = locationData['Node'].lstrip('n')
        return ret


    def getGpuTemperature(self, nodes: List[Node], from_dt: datetime.datetime, to_dt: datetime.datetime) -> GPUTemeraturepData:
        query = {
            "bool": {
                "must":[
                    {"terms":{"nid": [x.nid.replace('nid','').lstrip('0') for x in nodes]}},
                    {"match":{"Sensor.PhysicalContext": "GPU"}},
                    {"match":{"MessageId":"CrayTelemetry.Temperature"}},
                ],
                "filter": [{
                    "range": {
                        "@timestamp": {
                            "format": "epoch_second",
                            "gte": int(from_dt.timestamp()),
                            "lt": int(to_dt.timestamp()),
                        }
                    }
                }]
            }
        }
        all_gpu_temp_data = self._crawl('.ds-metrics-facility.telemetry-alps*', query)
        return GPUTemeraturepData(all_gpu_temp_data)

    def _crawl(self, index: str, query: Dict[str, Any], limit: int=-1) -> List[ElasticHit]:
        all_hits = []

        fetch_num = 10000
        endpoint = f'{self._base_url}/{index}/_search?scroll=1m'
        search_params = {
            "size": fetch_num,
            "query": query,
        }

        has_more_data = True
        while has_more_data:
            r = self._session.post(endpoint, json=search_params)
            if r.status_code >= 400:
                logger.error(f'Failed crawling with query={query}. Returned statuscode={r.status_code}, return error={r.text}')
            r.raise_for_status()

            resp = r.json()
            logger.debug(f'Crawling for {search_params=} took {resp["took"]}ms. {resp["hits"]["total"]=}')
            all_hits.extend(cast(List[ElasticHit], resp['hits']['hits']))

            if '_scroll_id' in resp and len(resp['hits']['hits']) == fetch_num and (limit==-1 or limit>len(all_hits)):
                endpoint = f'{self._base_url}/_search/scroll?scroll=1m'
                search_params = {
                    "scroll_id": resp['_scroll_id']
                }
            else:
                has_more_data = False

        return all_hits


def _to_local_datetime(dt: str) -> datetime.datetime:
    d = datetime.datetime.fromisoformat(dt)
    if d.tzinfo == None:
        # if no timezone information available, assume that the timestamp is in UTC
        d = datetime.datetime.fromisoformat(dt+'.000Z')
    return datetime.datetime.fromtimestamp(d.timestamp())


# this function expects the input to be in the form of 'nid[005560-005567,005571-005581,001234]' or 'nid001234,nid002345'
# or combinations of these two
# This is working correctly: `nodes=expand_nodes('nid00[5560-5567,5571],asd[001234-001235],qqr006789')`
def expand_nodes(nodes: str) -> List[str]:
    split_nodes = nodes.split(',')
    prefix = ''
    nodelist = []
    for node in split_nodes:
        reset_prefix = False
        if node.find('[') != -1:
            prefix, node = node.split('[')
        if node.find(']') != -1:
            node = node[:-1]
            reset_prefix = True
        if node.find('-') != -1:
            start, end = node.split('-')
            start_int = int(start.lstrip('0'))
            end_int = int(end.lstrip('0'))
            for node_nbr in range(start_int, end_int+1):
                nodelist.append(f'{{}}{{:0{len(start)}d}}'.format(prefix, node_nbr))
        else:
            nodelist.append(f'{prefix}{node}')
        if reset_prefix:
            prefix = ''
    return nodelist
