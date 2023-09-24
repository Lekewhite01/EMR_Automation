import boto3

def lambda_handler(event, context):
    # Initialize the EMR client
    emr_client = boto3.client('emr')

    # Specify the EMR cluster configuration
    cluster_config = emr_client.run_job_flow(
        Name='data_ml_v4', 
        ReleaseLabel='emr-6.10.0', 
        LogUri='s3://aws-logs-101063123548-eu-west-1/elasticmapreduce/',
        Instances={
        'InstanceGroups': [
            {
                'Name': "Master nodes",
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.12xlarge',
                'BidPrice': '1',
                'InstanceCount': 1,
            },
            {
                'Name': "Slave nodes",
                'Market': 'SPOT',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.12xlarge',
                'BidPrice': '1',
                'InstanceCount': 2,
            },
            {
                'Name': "Task nodes",
                'Market': 'SPOT',
                'InstanceRole': 'TASK',
                'InstanceType': 'm5.xlarge',
                'BidPrice': '0.2',
                'InstanceCount': 2,
            }
        ],
        'Ec2KeyName': 'emr_cluster',
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
        'Ec2SubnetId': 'subnet-e484d98f'
    },
    Steps=[
            {
        'Name': 'SparkJob',
        'ActionOnFailure': 'TERMINATE_CLUSTER',  # You can change this depending on your error handling strategy
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--master', 'yarn',
                '--deploy-mode', 'cluster',
                '--driver-cores', '40',
                '--executor-cores', '40',
                '--num-executors', '80',
                '--executor-memory', '100g',
                '--driver-memory', '50g',
                '--py-files', 's3://datateam-ml/Insights_Extraction/Ner/ner_dependency.zip', 's3://datateam-ml/Insights_Extraction/Ner/ner_script.py'
            ]

        }
    }
        ],
    BootstrapActions=[
        {
            'Name': 'install_dependencies',
            'ScriptBootstrapAction': {
                'Path': 's3://datateam-ml/Insights_Extraction/data_ml_emr.sh'  # S3 path to your bootstrap script
            }
        }
    ],
    Applications=[
            {'Name': 'Hadoop'},
            {'Name': 'Spark'}
        ], 
    ServiceRole='EMR_DefaultRole',  
    JobFlowRole='EMR_EC2_DefaultRole',  
    VisibleToAllUsers= True,
    Tags=[
        {
            'Key': 'terminateIfIdleForMins',
            'Value': 'kill'
        },
    ]
)
  
    # Get the cluster ID
    cluster_id = cluster_config['JobFlowId']
    
    return {
        'statusCode': 200,
        'body': f'Created EMR cluster with ID: {cluster_id} and submitted Spark job.'
    }

