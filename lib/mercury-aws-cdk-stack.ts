import * as cdk from '@aws-cdk/core';
import {CfnJob, CfnWorkflow, CfnTrigger} from '@aws-cdk/aws-glue';
import * as s3 from '@aws-cdk/aws-s3';
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




  }
}

