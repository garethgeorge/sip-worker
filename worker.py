import aiobotocore
import asyncio
import botocore 
import json 
import os 
import itertools 
import subprocess 
import time
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta

from config import config 

if not os.path.isdir("state"):
    os.mkdir("state")
if not os.path.isdir("state/bmbp_ts"):
    os.mkdir("state/bmbp_ts")
if not os.path.isdir("state/aws"):
    os.mkdir("state/aws")

RED   = "\033[1;31m"  
BLUE  = "\033[1;34m"
CYAN  = "\033[1;36m"
GREEN = "\033[0;32m"
RESET = "\033[0;0m"
BOLD    = "\033[;1m"
REVERSE = "\033[;7m"
worker_colors = [BOLD, RED, BLUE, CYAN, GREEN]
color_clear = RESET

# TODO: make the workers actually process stuff
epoch = datetime.utcfromtimestamp(0)
epoch.replace(tzinfo=None)
def util_time_to_epoc(datetime):
    return (datetime.replace(tzinfo=None) - epoch).total_seconds()
    
async def worker_entrypoint(loop):
    global work_queue 
    print("initializing work queue.")
    
    work_queue = asyncio.PriorityQueue(loop=loop)

    with open("instance-types.json", "r") as f:
        for job in json.load(f):
            await work_queue.put((0, job))
    
    print("\tLoaded %d jobs." % (work_queue.qsize(),))

    # spawn off a bunch of workers pulling from the work queue...
    for x in range(0, config["workers"]):
        print("\tstarted a worker... %d" % x)
        asyncio.ensure_future(Worker(x, work_queue, loop).run())

class Worker(object):
    def __init__(self, id, work_queue, loop):
        self.worker_id = id 
        self.work_queue = work_queue 
        self.loop = loop 
        self.worker_color = worker_colors[self.worker_id % len(worker_colors)]

    def log(self, *args):
        args = (self.worker_color,) + args + (color_clear,)
        print(*args)

    async def run(self):
        while True:
            last_time, job = await self.work_queue.get()
            wait_time = ((last_time + config["recheck_interval"]) - time.time()) # don't check more often than every sixty seconds
            if wait_time > 0:
                self.log("Worker %d took job, waiting %d seconds to execute. " % (self.worker_id, wait_time))
                await asyncio.sleep(abs(wait_time))
                self.log("Worker %d done waiting, executing job now." % self.worker_id)
            else:
                self.log("Worker %d took job. Executing immediately" % self.worker_id)

            task = WorkerTask(self, job)
            await task.run()
            await self.work_queue.put((time.time(), job))
            self.log("Worker %d done." % self.worker_id)

class WorkerTask(object):
    def __init__(self, worker, job):
        self.worker = worker
        self.job = job 
        
        self.tag = "%s.%s.%s" % (job["region"], job["az"], job["instance_type"])
        self.prefix = "W%d %s-%s" % (self.worker.worker_id, job["az"], job["instance_type"])
        self.state_dir = "state/%s.%s.%s/" % (job["region"], job["az"], job["instance_type"])
        if not os.path.isdir(self.state_dir):
            os.mkdir(self.state_dir)

        self.session = aiobotocore.get_session(loop=self.worker.loop)

    async def run(self):
        job = self.job 
        async with self.session.create_client("ec2", region_name=job["region"],
                aws_access_key_id=config["amazon"]["accessKeyId"],
                aws_secret_access_key=config["amazon"]["secretAccessKey"]) as client:
            if os.path.isfile(os.path.join(self.state_dir, "history.json")):
                with open(os.path.join(self.state_dir, "history.json"), "r") as f:
                    history_prior_obj = json.load(f)
                    history_prior = history_prior_obj["history"]
                    start_time = history_prior_obj["end_time"]
            else:
                history_prior = []
                start_time = None
            
            if start_time:
                start_time, end_time, history_latest = await self.get_spot_price_history(client, start_time_string=start_time)
            else:
                start_time, end_time, history_latest = await self.get_spot_price_history(client, history_seconds=config["history_window"])
            self.worker.log("\t%s fetched %d records of spot price data. " % (self.prefix, len(history_latest)))
            self.worker.log("\t%s diffing spot price data against prior records. " % (self.prefix,))
            

            hp_t = set(self.history_record_to_tupple(rec) for rec in history_prior)
            history_new = [record for record in history_latest if self.history_record_to_tupple(record) not in hp_t]
            print(history_new)

            with open(os.path.join(self.state_dir, "history.json"), "w") as f:
                json.dump({
                    "history": history_latest,
                    "end_time": end_time,
                    "start_time": start_time,
                }, f)
        
    async def get_spot_price_history(self, client, history_seconds=None, start_time_string=None):
        pag = client.get_paginator('describe_spot_price_history')

        end_time = datetime.now()
        if start_time_string == None:
            assert(history_seconds != None)
            start_time = end_time - timedelta(seconds = history_seconds)
            start_time_string = start_time.strftime("%Y-%m-%dT%H:%M:%S")
        end_time_string = end_time.strftime("%Y-%m-%dT%H:%M:%S")

        params = {
            'EndTime': end_time_string,
            'StartTime': start_time_string,
            'ProductDescriptions': ['Linux/UNIX'],
            'AvailabilityZone': self.job["az"],
            'InstanceTypes': [self.job["instance_type"]]
        }

        iterator = pag.paginate(**params)
        spot_prices = []
        async for page in iterator:
            for spotdata in page['SpotPriceHistory']:
                nicedata = {
                    "InstanceType": spotdata["InstanceType"],
                    "ProductDescription": spotdata["ProductDescription"],
                    "SpotPrice": float(spotdata["SpotPrice"]),
                    "Timestamp": spotdata["Timestamp"].strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                }
                spot_prices.append(nicedata)
        return start_time_string, end_time_string, spot_prices
    
    def history_tuple_to_dict(self, record):
        return {
            "InstanceType": record[1],
            "ProductDescription": record[2],
            "SpotPrice": record[3],
            "Timestamp": record[4]
        }

    def history_record_to_tupple(self, record):
        return (self.job["az"], record["InstanceType"], record["ProductDescription"], float(record["SpotPrice"]), record["Timestamp"])

    def history_tuple_to_line(self, record):
        return "SPOTPRICEHISTORY\t%s\t%s\t%s\t%.4f\t%s" % (record[0], record[1], record[2], record[3], record[4], record[5])
        # return string.Template("SPOTPRICEHISTORY\t$az\t$instance_type\t$product_description\t$spot_price\t$timestamp"")

def historyRecordsToTupples(historyRecords):
    return [(record["time"], record["spotprice"], record["type"]) for record in historyRecords]

def historyRecordsFromTupples(historyRecords):
    return [{"time": record[0], "spotprice": record[1], "type": record[2]} for record in historyRecords]

async def worker(loop, workerId):
    while True:
        last_time, job = await work_queue.get()
        wait_time = (time.time() - (last_time + 5)) # don't check more often than every sixty seconds
        if wait_time < 0:
            print("Worker %d took job, waiting %d seconds to execute. " % (workerId, abs(wait_time)))
            await asyncio.sleep(abs(wait_time))
            print("Worker %d done waiting, executing job now." % workerId)
        else:
            print("Worker %d took job. Executing immediately" % workerId)

        session = aiobotocore.get_session(loop=loop)

        async with session.create_client("ec2", region_name=job["region"],
                aws_access_key_id=config["amazon"]["accessKeyId"],
                aws_secret_access_key=config["amazon"]["secretAccessKey"]) as client:
            tag = "%s.%s.%s" % (job["region"], job["az"], job["instance_type"])
            state_dir = "state/%s.%s.%s/" % (job["region"], job["az"], job["instance_type"])
            if not os.path.isdir(state_dir):
                os.mkdir(state_dir)
            
            # collect the data 
            print("collecting data... " + tag)
            try:
                history_full = await client.describe_spot_price_history(
                    AvailabilityZone=job["az"],
                    InstanceTypes=[job["instance_type"]]
                )
                print(history_full['NextToken'])
                history_full = history_full['SpotPriceHistory']
            except botocore.exceptions.ClientError as e:
                work_queue.put((time.time(), job))
                continue 
            history_full = [{"time": int(util_time_to_epoc(record["Timestamp"])),
                             "spotprice": float(record["SpotPrice"]), 
                             "type": str(record["ProductDescription"])} for record in history_full]

            # diff the data against the prior state 
            print("diffing data ... " + tag)
            history_file = os.path.join(state_dir, "history.json")
            if os.path.isfile(history_file):
                with open(history_file, "r") as f:
                    history_full_prior = json.load(f)
            else:
                history_full_prior = None

            if history_full_prior:
                pft_t = set(historyRecordsToTupples(history_full_prior))
                ft_t = historyRecordsToTupples(history_full)
                history_new = [record for record in pft_t if record not in pft_t]
                history_new = historyRecordsFromTupples(history_new)
            else:
                history_new = history_full
            
            # if the diff is large enough do processing, otherwise we should pass on this loop and wait
            if len(history_new) > 100:
                data = sorted(history_new, key=lambda record: (record["type"], record["time"]))
                print(data)

                with open(history_file, "w") as f:
                    json.dump(history_full, f)

                for type, group in itertools.groupby(data, key=(lambda record: record["type"])):
                    bmbpts_input = os.path.join(state_dir, type + "-bmbp_ts_input.txt")
                    bmbpts_state = os.path.join(state_dir, type + "-state.bmbpts")
                    with open(bmbpts_input) as f:
                        f.writelines("%s %s" % (record["time"], record["type"]) for record in group)

                    args = ["./bin/bmbp_ts", "-f", bmbpts_input]
                    if os.path.isfile(bmbpts_state):
                        args += ["--loadstate", bmbpts_state]
                    args += ["--savestate", bmbpts_state]
                    args += ["-T"]

                    p = subprocess.Popen(args, stdout=subprocess.PIPE)
                    p.wait()
                    print(p.stdout.read())

        await work_queue.put((time.time(), job))
