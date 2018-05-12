import argparse
import os
import time
import math
import logging
import copy
import threading

import netaddr
import boto3
import namesgenerator
import paramiko
from scp import SCPClient
import requests

#Ab stands for aws benchmark

class Abclient(object):
    def __init__(self, args, log_handler):
        self.args = args
        self.init_args()
        self.log_handler = log_handler
        self.ec2client = boto3.client('ec2')

    def init_args(self):
        args = self.args
        if not args.key_name or not args.security_group_id:
            raise ValueError("key_name and security_group_id are required")

        if not args.task_name:
            args.task_name = self.generate_task_name()
            logging.info("task name generated %s" % (args.task_name))

        if not args.pem_path:
            args.pem_path = os.path.expanduser("~") + "/" + args.key_name + ".pem"
        if args.security_group_id:
            args.security_group_ids = (args.security_group_id, )
    
    def generate_task_name(self):
        return namesgenerator.get_random_name()
    
    def create_subnet(self):
        args = self.args
        # if no vpc id provided, list vpcs
        logging.info("start creating subnet")
        if not args.vpc_id:
            logging.info("no vpc provided, trying to find the default one")
            vpcs_desc = self.ec2client.describe_vpcs(
                Filters=[{
                    "Name": "isDefault",
                    "Values": ["true", ]
                }], )
            if len(vpcs_desc["Vpcs"]) == 0:
                raise ValueError('No default VPC')
            args.vpc_id = vpcs_desc["Vpcs"][0]["VpcId"]
            vpc_cidrBlock = vpcs_desc["Vpcs"][0]["CidrBlock"]

            logging.info("default vpc fount with id %s and CidrBlock %s" %
                        (args.vpc_id, vpc_cidrBlock))

        if not vpc_cidrBlock:
            logging.info("trying to find cidrblock for vpc")
            vpcs_desc = self.ec2client.describe_vpcs(
                Filters=[{
                    "Name": "vpc-id",
                    "Values": [args.vpc_id, ],
                }], )
            if len(vpcs_desc["Vpcs"]) == 0:
                raise ValueError('No VPC found')
            vpc_cidrBlock = vpcs_desc["Vpcs"][0]["CidrBlock"]
            logging.info("cidrblock for vpc is %s" % vpc_cidrBlock)

        # list subnets in vpc in order to create a new one

        logging.info("trying to find ip blocks for new subnet")
        subnets_desc = self.ec2client.describe_subnets(
            Filters=[{
                "Name": "vpc-id",
                "Values": [args.vpc_id, ],
            }], )

        ips_taken = []
        for subnet_dec in subnets_desc["Subnets"]:
            ips_taken.append(subnet_dec["CidrBlock"])

        ip_blocks_avaliable = netaddr.IPSet(
            [vpc_cidrBlock]) ^ netaddr.IPSet(ips_taken)
        # adding 10 addresses as buffer
        cidr_prefix = 32 - math.ceil(
            math.log(args.pserver_count + args.trainer_count + 10, 2))
        if cidr_prefix <= 16:
            raise ValueError('Too many nodes to fit in current VPC')

        for ipnetwork in ip_blocks_avaliable.iter_cidrs():
            try:
                subnet_cidr = ipnetwork.subnet(int(cidr_prefix)).next()
                logging.info("subnet ip block found %s" % (subnet_cidr))
                break
            except Exception:
                pass

        if not subnet_cidr:
            raise ValueError(
                'No avaliable subnet to fit required nodes in current VPC')

        logging.info("trying to create subnet")
        subnet_desc = self.ec2client.create_subnet(
            CidrBlock=str(subnet_cidr),
            VpcId=args.vpc_id,
            AvailabilityZone=args.availability_zone)

        subnet_id = subnet_desc["Subnet"]["SubnetId"]

        subnet_waiter = self.ec2client.get_waiter('subnet_available')
        # sleep for 1s before checking its state
        time.sleep(1)
        subnet_waiter.wait(SubnetIds=[subnet_id, ])

        logging.info("subnet created")

        logging.info("adding tags to newly created subnet")
        self.ec2client.create_tags(
            Resources=[subnet_id, ],
            Tags=[{
                "Key": "Task_name",
                'Value': args.task_name
            }])
        return subnet_id

    def run_instances(self, image_id, instance_type, count=1, role="MASTER", cmd=""):
        args = self.args
        ec2client = self.ec2client
        response = ec2client.run_instances(
            ImageId=image_id,
            InstanceType=instance_type,
            MaxCount=count,
            MinCount=count,
            UserData=cmd,
            DryRun=False,
            InstanceInitiatedShutdownBehavior="stop",
            KeyName=args.key_name,
            Placement={'AvailabilityZone': args.availability_zone},
            NetworkInterfaces=[{
                'DeviceIndex': 0,
                'SubnetId': args.subnet_id,
                "AssociatePublicIpAddress": True,
                'Groups': args.security_group_ids
            }],
            TagSpecifications=[{
                'ResourceType': "instance",
                'Tags': [{
                    "Key": 'Task_name',
                    "Value": args.task_name + "_master"
                }, {
                    "Key": 'Role',
                    "Value": role
                }]
            }])

        instance_ids = []
        for instance in response["Instances"]:
            instance_ids.append(instance["InstanceId"])

        if len(instance_ids) > 0:
            logging.info(str(len(instance_ids)) + " instance(s) created")
        else:
            logging.info("no instance created")
        #create waiter to make sure it's running

        logging.info("waiting for instance to become accessible")
        waiter = ec2client.get_waiter('instance_status_ok')
        waiter.wait(
            Filters=[{
                "Name": "instance-status.status",
                "Values": ["ok"]
            }, {
                "Name": "instance-status.reachability",
                "Values": ["passed"]
            #}, {
            #    "Name": "instance-state-name",
            #    "Values": ["running"]
            }],
            InstanceIds=instance_ids)

        instances_response = ec2client.describe_instances(InstanceIds=instance_ids)

        return instances_response["Reservations"][0]["Instances"]

    def create(self):
        args = self.args
        self.init_args()

        # create subnet
        if not args.subnet_id:
            args.subnet_id = self.create_subnet()

        # create master node

        master_instance_response = self.run_instances(
            image_id="ami-7a05351f", instance_type="t2.nano")

        logging.info("master server started")

        args.master_server_public_ip = master_instance_response[0][
            "PublicIpAddress"]
        args.master_server_ip = master_instance_response[0]["PrivateIpAddress"]

        logging.info("master server started, master_ip=%s, task_name=%s" %
                    (args.master_server_public_ip, args.task_name))

        # cp config file and pems to master node

        ssh_key = paramiko.RSAKey.from_private_key_file(args.pem_path)
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(
            hostname=args.master_server_public_ip, username="ubuntu", pkey=ssh_key)

        with SCPClient(ssh_client.get_transport()) as scp:
            scp.put(os.path.expanduser("~") + "/" + ".aws",
                    recursive=True,
                    remote_path='/home/ubuntu/')
            scp.put(args.pem_path,
                    remote_path='/home/ubuntu/' + args.key_name + ".pem")

        logging.info("credentials and pem copied to master")

        # set arguments and start docker
        if args.online_mode:
            kick_off_cmd = "docker run -i -v /home/ubuntu/.aws:/root/.aws/"
        else:
            kick_off_cmd = "docker run -d -v /home/ubuntu/.aws:/root/.aws/"
            
        kick_off_cmd += " -v /home/ubuntu/" + args.key_name + ".pem:/root/" + args.key_name + ".pem"
        kick_off_cmd += " -v /home/ubuntu/logs/:/root/logs/"
        kick_off_cmd += " -p " + str(args.master_server_port) + ":" + str(
            args.master_server_port)
        kick_off_cmd += " " + args.master_docker_image

        args_to_pass = copy.copy(args)
        args_to_pass.action = "create"
        del args_to_pass.pem_path
        del args_to_pass.security_group_ids
        del args_to_pass.master_docker_image
        del args_to_pass.master_server_public_ip
        for arg, value in sorted(vars(args_to_pass).iteritems()):
            if value:
                kick_off_cmd += ' --%s %s' % (arg, value)

        logging.info(kick_off_cmd)
        stdin, stdout, stderr = ssh_client.exec_command(command=kick_off_cmd)
        
        if self.args.online_mode:
            stdout_thread = threading.Thread(
                target=self.log_handler,
                args=(
                    stdout,
                    "stdout", ))
            stderr_thread = threading.Thread(
                target=self.log_handler,
                args=(
                    stderr,
                    "stderr", ))
            stdout_thread.start()
            stderr_thread.start()

            stdout_thread.join()
            stderr_thread.join()

        return_code = stdout.channel.recv_exit_status()
        logging.info(return_code)
        if return_code != 0:
            raise Exception("Error while kicking off master")
        if self.args.online_mode:
            logging.info("training task finished, going to clean up instances")
            self.cleanup()
        else:
            logging.info(
                "master server finished init process, visit %s to check master log" %
                (self.get_master_web_url("/status")))
    
    def _hard_cleanup(self):
        args = self.args
        task_name = args.task_name
        ec2client = self.ec2client
        if args.no_clean_up:
            logging.info("no clean up option set, going to leave the setup running")
            return
        #shutdown all ec2 instances
        print("going to clean up " + task_name + " instances")
        instances_response = ec2client.describe_instances(Filters=[{
            "Name": "tag:Task_name",
            "Values": [task_name, task_name + "_master"]
        }])

        instance_ids = []
        if len(instances_response["Reservations"]) > 0:
            for reservation in instances_response["Reservations"]:
                for instance in reservation["Instances"]:
                    instance_ids.append(instance["InstanceId"])

            ec2client.terminate_instances(InstanceIds=instance_ids)

            instance_termination_waiter = ec2client.get_waiter(
                'instance_terminated')
            instance_termination_waiter.wait(InstanceIds=instance_ids)

        #delete the subnet created

        subnet = ec2client.describe_subnets(Filters=[{
            "Name": "tag:Task_name",
            "Values": [task_name]
        }])

        if len(subnet["Subnets"]) > 0:
            ec2client.delete_subnet(SubnetId=subnet["Subnets"][0]["SubnetId"])
        # no subnet delete waiter, just leave it.
        logging.info("Clearnup done")
        return
        

    def cleanup(self):
        if self.args.online_mode:
            logging.info("online mode: true, hard cleanup")
            self._hard_cleanup()
        else:
            logging.info("online mode: false, keep master running")
            print requests.post(self.get_master_web_url("/cleanup")).text


    def status(self):
        print requests.post(self.get_master_web_url("/status")).text


    def get_master_web_url(self, path):
        args = self.args
        return "http://" + args.master_server_public_ip + ":" + str(
            args.master_server_port) + path