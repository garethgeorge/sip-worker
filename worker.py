import aiobotocore
import asyncio
import botocore 
import json 
import os 

from config import config 

os.mkdir("state")
os.mkdir("state/bmbp_ts")

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
        
        session = aiobotocore.get_session(loop=loop)

        async with session.create_client('ec2', region_name=job["region"], 
                aws_access_key_id=config["amazon"]["accessKeyId"],
                aws_secret_access_key=config["amazon"]["secretAccessKey"]) as client:
            try:
                history = await client.describe_spot_price_history(
                    AvailabilityZone=job["az"],
                    InstanceTypes=[job["instance_type"]]
                )
                print(history)

            except botocore.exceptions.ClientError as e:
                print("ClientError on job: %s" % (str(e),))
                continue 
            
            except Exception as e:
                print("Worker encountered an exception while processing job %s" % (str(job)))
                print(e)
                continue 

            finally:

                print("Replaced job in work queue.")
                await work_queue.put(job)

