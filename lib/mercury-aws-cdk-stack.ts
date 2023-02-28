import * as cdk from '@aws-cdk/core';
import * as glue from '@aws-cdk/aws-glue';
import * as s3 from '@aws-cdk/aws-s3';
import * as lambda from '@aws-cdk/aws-lambda';
import * as lambdaEventSources from '@aws-cdk/aws-lambda-event-sources';
import * as iam from '@aws-cdk/aws-iam';
import * as s3deploy from '@aws-cdk/aws-s3-deployment';
import { Construct } from 'constructs';


export class MercuryAwsCdkStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const project_name = 'cdk-mercury-daily-files'

	const env_config = {
		account_id: '464340339497',
		account_region: 'us-east-1',
		redshift_host: 'redshift-cluster-1.cwxx3jnng4yo.us-east-1.redshift.amazonaws.com',
		redshift_db_port: '5439',
		redshift_db_user: 'awsuser',
		redshift_db_pwd: 'AWSUser5439',
		redshift_db_name: 'dev',
		redshift_cluster_identifier: 'redshift-cluster-1',
		redshift_cluster_host_name: 'redshift-cluster-1.cwxx3jnng4yo.us-east-1.redshift.amazonaws.com',
		redshift_api_role: 'arn:aws:iam::464340339497:role/Redshift-data-api-role',
		vpc_connection: 'load-redshift',
		env_role: 'arn:aws:iam::464340339497:role/service-role/AWSGlueServiceRole-Datalake'
	  }

	const workflow_config = {
		// project_name: 'cdk-mercury-daily-files',
		temp_bucket_name: `${project_name}-temp`,
		destination_bucket_name: `${project_name}-csv`,
		glue_job_run_sql_sp: `${project_name}-output`,
		lambda_name_rename_move_files: `${project_name}-rename-move`, 
		etl_workflow_name: `${project_name}-etl-workflow`,
		etl_workflow_desc: 'Generate Daily Files for Mercury Team - Workflow Automation AWS CDK'
	  }

	const temp_bucket = new s3.Bucket(this, `${workflow_config.temp_bucket_name}`,
	  {
		bucketName: `${workflow_config.temp_bucket_name}`,
		/* The following properties ensure the bucket is properly 
		 * deleted when we run cdk destroy */
		removalPolicy: cdk.RemovalPolicy.DESTROY
	  });

	  const destination_bucket = new s3.Bucket(this, `${workflow_config.destination_bucket_name}`,
    {
      bucketName: `${workflow_config.destination_bucket_name}`
    });

	const glue_job_run_sql_stored_procedure = new glue.CfnJob(this, `${workflow_config.glue_job_run_sql_sp}`, {
		name: `${workflow_config.glue_job_run_sql_sp}`,
		description: 'glue job to run SQL stored procedure',
		command: {
		  name: 'pythonshell',
		  pythonVersion: '3',
		  scriptLocation: 's3://cdk-mercury-daily-files-temp/scripts/cdk_lambda_rename_move_files.py'
		},
		
		role: `${env_config.env_role}`,
		defaultArguments: {
		  '--TempDir': `s3://aws-glue-temporary-${env_config.account_id}-${env_config.account_region}/`,
		  '--additional-python-modules': 'aws-psycopg2',
		  '--dbname': `${env_config.redshift_db_name}`,
		  '--host': `${env_config.redshift_cluster_host_name}`,
		  '--password': env_config.redshift_db_pwd, 
		  '--port': env_config.redshift_db_port, 
		  '--user': env_config.redshift_db_user,
		  '--s3_load_sql_bucket': workflow_config.temp_bucket_name, 
		  '--s3_load_sql_key': 'sql/sp_mercury_jan_2023.sql',
		  '--enable-metrics': 'true',
		  '--enable-continuous-cloudwatch-log': 'true',
		  '--job-language': 'python',
		  '--enable-glue-datacatalog': 'false'
		},
		connections: {
		  connections: [`${env_config.vpc_connection}`],
		},
		glueVersion: "3.0",
		maxRetries: 0,
		maxCapacity: 1,
		timeout: 300
	  });
  
	const lambda_rename_move_files = new lambda.Function(this, `${workflow_config.lambda_name_rename_move_files}`, {
		runtime: lambda.Runtime.PYTHON_3_7, //execution enviroment
		code: lambda.Code.fromAsset("lib/assets"),  //directory used from where code is loaded
		handler: 'lambda_rename_move_files.lambda_handler', //name of file.function is lambda_handler in the code for lambda
		timeout: cdk.Duration.minutes(10),
		  });
  
  
	  // create a policy statement
	  // for giving permissions to the lambda to run the glue job
	const lambda_permissions_to_s3 = new iam.PolicyStatement({
		actions: ['s3:*'],
		resources: ['*'],
	  });
  
	// add the policy to the Function's role
	 // This provides access to the lambda function to S3 bucket
	 lambda_rename_move_files.role?.attachInlinePolicy(
	  new iam.Policy(this, `${project_name}-s3-permissions-to-lambda`, {
		statements: [lambda_permissions_to_s3],
	  }),
	);
  
	// Adding a trigger to the lambda function. The function is triggered as soon as a file is added to the bucket
	const s3PutEventSource = new lambdaEventSources.S3EventSource( temp_bucket , {
	  events: [
		s3.EventType.OBJECT_CREATED_PUT
	  ]
	});

	lambda_rename_move_files.addEventSource(s3PutEventSource);





  }
}

