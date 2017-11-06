import aiobotocore
import asyncio
import botocore 
import json 
import os 
from datetime import datetime, timezone 
from dateutil.relativedelta import relativedelta

epoch = datetime.utcfromtimestamp(0)
epoch.replace(tzinfo=None)

from config import config 

if not os.path.isdir("state"):
    os.mkdir("state")
if not os.path.isdir("state/bmbp_ts"):
    os.mkdir("state/bmbp_ts")
if not os.path.isdir("state/aws"):
    os.mkdir("state/aws")

# TODO: make the workers actually process stuff

async def worker_entrypoint(loop):
    global work_queue 
    print("initializing work queue.")
    
    work_queue = asyncio.Queue(loop=loop)

    with open("instance-types.json", "r") as f:
        for job in json.load(f):
            await work_queue.put(job)
    
    print("\tLoaded %d jobs." % (work_queue.qsize(),))

    # spawn off a bunch of workers pulling from the work queue...
    for x in range(0, config["workers"]):
        asyncio.ensure_future(worker(loop, x))

async def worker(loop, workerId):
    while True:
        job = await work_queue.get()
        print("Worker %d took a job." % workerId)
        
        session = aiobotocore.get_session(loop=loop)

        async with session.create_client('ec2', region_name=job["region"], 
                aws_access_key_id=config["amazon"]["accessKeyId"],
                aws_secret_access_key=config["amazon"]["secretAccessKey"]) as client:
            try:
                
                state_file_name = "state/aws/" + job["region"] + "_" + job["az"] + "_" + job["instance_type"]

                last_history = None 
                if os.path.isfile(state_file_name):
                    with open(state_file_name, "r") as f:
                        aws_state = json.load(f)
                    
                    last_history = [(row[0], row[1], row[2]) for row in aws_state["history"]]
                    start_time = datetime.fromtimestamp(last_history[-1][0])
                
                if last_history == None or len(last_history) == 0:
                    print("hitting this case... what the heck dood.")
                    last_history = []
                    start_time = datetime.today() + relativedelta(months=-3)

                print("Requesting history after start time %s" % str(start_time))
                history = await client.describe_spot_price_history(
                    AvailabilityZone=job["az"],
                    InstanceTypes=[job["instance_type"]],
                    StartTime = start_time,
                    EndTime = datetime.today()
                )

                last_history_set = set(last_history)
                new_history = [((row["Timestamp"].replace(tzinfo=None) - epoch).total_seconds(), row['ProductDescription'], row['SpotPrice']) for row in history["SpotPriceHistory"]]
                new_history.sort()

                new_history_diff = list(filter(lambda row: row not in last_history_set, new_history))

                print("Retreived %d rows, diff size %d" % (len(new_history), len(new_history_diff)))

                if len(new_history_diff) > 0:
                    print("new history!")
                    for record in new_history_diff:
                        print(record)

                    with open(state_file_name, 'w') as f:
                        json.dump({
                            "history": new_history,
                        }, f)
                else:
                    print("no new history... requeing job until new history is available.")
                
            except botocore.exceptions.ClientError as e:
                print("ClientError on job: %s" % (str(e),))
        
        await work_queue.put(job)
