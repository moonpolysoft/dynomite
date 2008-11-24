""" This functional test may be run under nosetests or by running this
module directly. When running under nosetests, The following
environment variables can be set to control test behavior and set ec2
connection parameters, in lieu of the command line options available
when run directly:

AWS_USERID              -- your amazon user id
AWS_KEY                 -- your amazon key 
AWS_SECRET_KEY          -- your amazon secret key
AWS_SSH_KEY             -- your amazon ssh public key

EC2_AMI                 -- your AMI name. AMI must have erlang R12B-1 or 
                           better, ruby, rake, python 2.5.1 or better, and
                           thrift installed
EC2_INSTANCE_TYPE       -- type of EC2 instances to start
EC2_INSTANCES           -- number of EC2 instances to start
EC2_RUN_TIME            -- length of time to run the load test
EC2_CLIENTS_PER_HOST    -- number of clients per instance
EC2_GET_THRESHOLD       -- If 99.9% of gets are not faster than this # of
                           milliseconds, the test fails
EC2_PUT_THRESHOLD       -- If 99.9% of puts are not faster than this # of
                           milliseconds, the test fails
"""
