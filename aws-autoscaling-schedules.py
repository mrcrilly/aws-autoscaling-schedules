import time
import datetime
import boto3
import yaml
import logging

def process_scheduling(environment):
    if not environment:
        return False

    datetime_format = "%Y-%m-%dT%H%M%S"
    asg = boto3.client("autoscaling")
    
    if not "asg" in environment:
        return False

    if environment["items"]:
        for schedule in environment["schedules"]:
            logging.info("Adding schedule %s..." % environment["asg"])

            try:
                asg.delete_scheduled_action(AutoScalingGroupName=environment["asg"], ScheduledActionName=schedule["name"])
                time.sleep(5) # this seems to be needed, but it's annoying. More research needed.
            except:
                pass

            if not "iso_date" in schedule:
                now = datetime.now()
                schedule["iso_date"] = "%s-%s-%sT%H0000" % (now.year, now.month, now.day, now.hour)

            response = asg.put_scheduled_update_group_action(
                AutoScalingGroupName=environment["asg"],
                ScheduledActionName=schedule["name"],
                StartTime=datetime.datetime.strptime(schedule["iso_date"], datetime_format),
                Recurrence=schedule["cronjob"] if "cronjob" in schedule else None,
                MinSize=schedule["min"] if "min" in schedule else 0,
                MaxSize=schedule["max"] if "max" in schedule else 4,
                DesiredCapacity=schedule["desired"] if "desired" in schedule else 2
            )

    return True

def main():
    with open("schedules.yaml", "r") as fd:
        configuration = yaml.load(fd)

        if configuration:
            for environment in configuration["environments"]:
                if not process_scheduling(environment):
                    logging.error("Some issue processing %s" % environment["asg"])
        else:
            print "Some error loading schedules.yaml"

if __name__ == "__main__":
    main()
