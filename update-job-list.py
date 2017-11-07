import aiobotocore
import asyncio
import botocore 
import itertools 
import json 

from config import config 

loop = asyncio.get_event_loop()

wanted_regions = ["us-east-2", "us-east-1", "us-west-1", "us-west-2"]

async def run():
    jobs = []

    session = aiobotocore.get_session(loop=loop)
    async with session.create_client("ec2", region_name="us-west-1",
            aws_access_key_id=config["amazon"]["accessKeyId"],
            aws_secret_access_key=config["amazon"]["secretAccessKey"]) as client:
        print("connected to amazon, got a session and all that jazz")

        print("regions:")
        for region_obj in (await client.describe_regions())["Regions"]:
            region_name = region_obj["RegionName"]
            if region_name not in wanted_regions: continue
            print("\t- %s" % region_name)

            async with session.create_client("ec2", region_name=region_name,
                    aws_access_key_id=config["amazon"]["accessKeyId"],
                    aws_secret_access_key=config["amazon"]["secretAccessKey"]) as regionalClient:
                for az in (await regionalClient.describe_availability_zones())["AvailabilityZones"]:
                    jobs.append({
                        "region": az["RegionName"],
                        "az": az["ZoneName"]
                    })

    with open("availability-zones.json", "w") as f:
        f.write(json.dumps(jobs, indent=2))

loop.run_until_complete(run())
loop.close()
