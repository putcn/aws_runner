import argparse
import logging
import sys
import numpy as np

# for ce env ONLY

sys.path.append(os.environ['ceroot'])
from kpi import LessWorseKpi

from abclient import Abclient

def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

parser = argparse.ArgumentParser(description=__doc__)

parser.add_argument(
    '--key_name', type=str, default="", help="required, key pair name")
parser.add_argument(
    '--security_group_id',
    type=str,
    default="",
    help="required, the security group id associated with your VPC")

parser.add_argument(
    '--vpc_id',
    type=str,
    default="",
    help="The VPC in which you wish to run test")
parser.add_argument(
    '--subnet_id',
    type=str,
    default="",
    help="The Subnet_id in which you wish to run test")

parser.add_argument(
    '--pserver_instance_type',
    type=str,
    default="c5.2xlarge",
    help="your pserver instance type, c5.2xlarge by default")
parser.add_argument(
    '--trainer_instance_type',
    type=str,
    default="p2.8xlarge",
    help="your trainer instance type, p2.8xlarge by default")

parser.add_argument(
    '--task_name',
    type=str,
    default="",
    help="the name you want to identify your job")
parser.add_argument(
    '--pserver_image_id',
    type=str,
    default="ami-da2c1cbf",
    help="ami id for system image, default one has nvidia-docker ready, \
    use ami-1ae93962 for us-east-2")

parser.add_argument(
    '--pserver_command',
    type=str,
    default="",
    help="pserver start command, format example: python,vgg.py,batch_size:128,is_local:yes"
)

parser.add_argument(
    '--trainer_image_id',
    type=str,
    default="ami-da2c1cbf",
    help="ami id for system image, default one has nvidia-docker ready, \
    use ami-1ae93962 for us-west-2")

parser.add_argument(
    '--trainer_command',
    type=str,
    default="",
    help="trainer start command, format example: python,vgg.py,batch_size:128,is_local:yes"
)

parser.add_argument(
    '--availability_zone',
    type=str,
    default="us-east-2a",
    help="aws zone id to place ec2 instances")

parser.add_argument(
    '--trainer_count', type=int, default=1, help="Trainer count")

parser.add_argument(
    '--pserver_count', type=int, default=1, help="Pserver count")

parser.add_argument(
    '--action', type=str, default="create", help="create|cleanup|status")

parser.add_argument('--pem_path', type=str, help="private key file")

parser.add_argument(
    '--pserver_port', type=str, default="5436", help="pserver port")

parser.add_argument(
    '--docker_image', type=str, default="busybox", help="training docker image")

parser.add_argument(
    '--master_server_port', type=int, default=5436, help="master server port")

parser.add_argument(
    '--master_server_public_ip', type=str, help="master server public ip")

parser.add_argument(
    '--master_docker_image',
    type=str,
    default="putcn/paddle_aws_master:latest",
    help="master docker image id")

parser.add_argument(
    '--no_clean_up',
    type=str2bool,
    default=False,
    help="whether to clean up after training")

parser.add_argument(
    '--online_mode',
    type=str2bool,
    default=False,
    help="is client activly stays online")

args = parser.parse_args()
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

train_speed_kpi = LessWorseKpi('train_speed', 0.01)
kpis_to_track = {}

def save_to_kpi(name, val):
    # currently only train speed, train_accuracy and test_accuracy are tracked
    # they are all LessWorseKpi type

    val = float(val)
    if name in kpis_to_track:
        kpi_to_track = kpis_to_track[name]
    else:
        kpi_to_track = LessWorseKpi(name, 0.01)
    kpi_to_track.add_record(np.array(val, dtype='float32'))

def log_handler(source, id):
    for line in iter(source.readline, ""):
        logging.info(line)
        if (line.startswith("**metrics_data: ")):
            # parse and save to kpi
            str_msg = line.replace("**metrics_data: ", "")
            metrics_raw = str_msg.split(",")
            for metric in metrics_raw:
                metric_data = metric.split("=")
                save_to_kpi(metric_data[0], metric_data[1])
                
            

abclient = Abclient(args, log_handler)

if __name__ == "__main__":
    print_arguments()
    if args.action == "create":
        abclient.create()