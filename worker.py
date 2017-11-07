import aiobotocore
import asyncio
import botocore 
import itertools 
import json 
import os 
import subprocess
import shutil 
import time
import logging
import aiofiles
from colors import *
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)

from config import config 

if not os.path.isdir("state"):
    os.mkdir("state")
if not os.path.isdir("tmp"):
    os.mkdir("tmp")

worker_colors = [BOLD, RED, BLUE, CYAN, GREEN]
color_clear = RESET

epoch = datetime.utcfromtimestamp(0)
epoch.replace(tzinfo=None)
def util_time_to_epoc(datetime):
    return (datetime.replace(tzinfo=None) - epoch).total_seconds()


async def worker_entrypoint(loop):
    global work_queue, cruncher_ids
    print("initializing work queue.")
    
    work_queue = asyncio.PriorityQueue(loop=loop)
    cruncher_ids = asyncio.Queue(loop=loop)

    with open("availability-zones.json", "r") as f:
        for id, job in enumerate(json.load(f)):
            await work_queue.put((0, id, job))
    
    print("\tLoaded %d jobs." % (work_queue.qsize(),))

    # spawn off a bunch of workers pulling from the work queue...
    print("\tfetch_workers")
    for x in range(0, config["fetch_workers"]):
        print("\tstarted a worker... %d" % x)
        asyncio.ensure_future(Worker(x, work_queue, loop).run())

    for x in range(0, config["crunch_workers"]):
        await cruncher_ids.put(x)

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
            last_time, jobid, job = await self.work_queue.get()
            wait_time = ((last_time + config["recheck_interval"]) - time.time()) # don"t check more often than every sixty seconds
            if wait_time > 0:
                self.log("Worker %d took job, waiting %d seconds to execute. " % (self.worker_id, wait_time))
                await asyncio.sleep(abs(wait_time))
                self.log("Worker %d done waiting, executing job now." % self.worker_id)
            else:
                self.log("Worker %d took job. Executing immediately" % self.worker_id)

            task = WorkerTask(self, job)
            # try: # in production we will want a try catch here... yep yep.
            await task.run()
            # except Exception as e:
            #     self.log("ERROR!!! ERROR!!! ERROR!!! %r" % (e,))
            await self.work_queue.put((time.time(), jobid, job))
            self.log("Worker %d done." % self.worker_id)

class WorkerTask(object):
    def __init__(self, worker, job):
        self.worker = worker
        self.job = job
        
        self.prefix = "W%d %s" % (self.worker.worker_id, job["az"])
        self.state_dir = "./state/%s.%s/" % (job["region"], job["az"])
        if not os.path.isdir(self.state_dir):
            os.mkdir(self.state_dir)

        self.session = aiobotocore.get_session(loop=self.worker.loop)

    async def run(self):
        job = self.job 
        async with self.session.create_client("ec2", region_name=job["region"],
                aws_access_key_id=config["amazon"]["accessKeyId"],
                aws_secret_access_key=config["amazon"]["secretAccessKey"]) as client:
            if os.path.isfile(os.path.join(self.state_dir, "history.json")):
                async with aiofiles.open(os.path.join(self.state_dir, "history.json"), "r") as f:
                    data = await f.read()
                    history_prior_obj = json.loads(data)
                    history_prior = history_prior_obj["history"]
                    start_time = history_prior_obj["end_time"]
            else:
                history_prior = []
                start_time = None
            
            self.worker.log("\t%s fetched fetching price data " % (self.prefix,))
            if start_time:
                start_time, end_time, history_latest = await self.get_spot_price_history(client, start_time_string=start_time)
            else:
                start_time, end_time, history_latest = await self.get_spot_price_history(client, history_seconds=config["history_window"])
            
            self.worker.log("\t%s got %d records, diffing spot price data against prior data" % (self.prefix,  len(history_latest)))

            hp_t = set(self.history_record_to_tupple(rec) for rec in history_prior)
            history_new = [record for record in history_latest if self.history_record_to_tupple(record) not in hp_t]

            if (await self.process_diff_batch(history_new)):
                # NOTE: we only want to dump the history if this operation succeeds, otherwise 
                # the history will get refetched and the diff reprocessed if there is an error
                async with aiofiles.open(os.path.join(self.state_dir, "history.json"), "w") as f:
                    await f.write(json.dumps({
                        "history": history_latest, # yes this is supposed to dump latest, not new
                        "end_time": end_time,
                        "start_time": start_time,
                    }))

    async def process_diff_batch(self, records):
        self.worker.log("\t%s processing diff: %d records, identifying instance types that need processing." % (self.prefix, len(records)))
        records = sorted(records, key=lambda record:(record["InstanceType"], record["Timestamp"]))

        work = []
        for instance_type, inst_records in itertools.groupby(records, key=lambda record:record["InstanceType"]):
            inst_records = list(inst_records)
            if len(inst_records) > 10:
                self.worker.log("\t\t%s instance %s needs an update, %d records changed" % (self.prefix, instance_type, len(inst_records)))
                work.append(asyncio.ensure_future(self.process_diff(instance_type, inst_records)))
        
        if len(work) > 0:
            self.worker.log("\t%s pondorously crunching the numbers. please hold." % (self.prefix))
            await asyncio.wait(work)
            self.worker.log("\t%s done crunching." % (self.prefix))

            return True
        else:
            self.worker.log("\t%s no numbers to crunch! Simply moving along." % (self.prefix))
            return False 

    async def process_diff(self, instance_type, records):
        
        print("blocking on request for an id")
        id = await cruncher_ids.get()
        self.worker.log("\t%s processing diff and making predictions. records: %d" % (self.prefix, len(records)))
        
        working_dir = "./tmp/worker-%x/" % id 
        data_file = os.path.join(working_dir, "data.txt")
        state_file = os.path.join(self.state_dir, "%s.state" % instance_type)
        results_dir = os.path.join(working_dir, "results")
        archive_results_dir = self.state_dir + "/results/" + instance_type + "/" + datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        os.makedirs(archive_results_dir, exist_ok=True)

        if os.path.isdir(working_dir):
            print("removing working directory...")
            shutil.rmtree(working_dir)
            print("done removing it...")
        os.makedirs(working_dir)


        # write out the input data
        async with aiofiles.open(data_file, "w") as f:
            for record in records:
                await f.write(self.history_tuple_to_line(self.history_record_to_tupple(record)) + "\n")

        self.worker.log("\t%s beginning to execute the shell script..." % (self.prefix,))

        # copy over the binary state file 
        args = ("sh", "./predict-savestate.sh")
        args += (working_dir,)
        args += (self.job["region"], self.job["az"])

        p = await asyncio.create_subprocess_exec(*args)
        returncode = await p.wait()
        
        self.worker.log("\t%s processed diff, exit status %d" % (self.prefix, returncode))
        
        # check that all of the expected files are there
        # TODO: copy in the state save and state restore stuff
        for path in [results_dir]:
            if not os.path.exists(path):
                cruncher_ids.put(id)
                raise Exception("when checking result files, did not get file \"%s\"" % (path,))
            else:
                self.worker.log("\t\t%s result \"%s\" OK" % (self.prefix, path))
        self.worker.log("\t%s archiving!" % (self.prefix,))

        for file in os.listdir(results_dir):
            self.worker.log("\t\t%s - archived %s" % (self.prefix, file))
            with open(results_dir + '/' + file, 'r') as rd:
                with open(archive_results_dir + '/' + file, 'w') as wr:
                    wr.write(rd.read())
        self.worker.log("\t%s done processing!" % (self.prefix,))

        await cruncher_ids.put(id)

        return 0
        
    async def get_spot_price_history(self, client, history_seconds=None, start_time_string=None):
        pag = client.get_paginator("describe_spot_price_history")

        end_time = datetime.now()
        if start_time_string == None:
            assert(history_seconds != None)
            start_time = end_time - timedelta(seconds = history_seconds)
            start_time_string = start_time.strftime("%Y-%m-%dT%H:%M:%S")
        end_time_string = end_time.strftime("%Y-%m-%dT%H:%M:%S")

        params = {
            "EndTime": end_time_string,
            "StartTime": start_time_string,
            "ProductDescriptions": ["Linux/UNIX"],
            "AvailabilityZone": self.job["az"],
            # "InstanceTypes": [self.job["instance_type"]]
        }

        iterator = pag.paginate(**params)
        spot_prices = []
        async for page in iterator:
            for spotdata in page["SpotPriceHistory"]:
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
        return "SPOTPRICEHISTORY\t%s\t%s\t%s\t%.4f\t%s" % record
        # return string.Template("SPOTPRICEHISTORY\t$az\t$instance_type\t$product_description\t$spot_price\t$timestamp"")
