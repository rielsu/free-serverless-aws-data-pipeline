import * as cdk from 'aws-cdk-lib'
import { type Construct } from 'constructs'
import type * as s3 from 'aws-cdk-lib/aws-s3'
import { type StageConfig } from './config-builder'
import * as lambda from 'aws-cdk-lib/aws-lambda'
import * as apigateway from 'aws-cdk-lib/aws-apigateway'
import * as ecrAssets from 'aws-cdk-lib/aws-ecr-assets'
import path = require('path')
import * as iam from 'aws-cdk-lib/aws-iam'

interface genericApiStackProps extends cdk.StackProps {
    dataLakeBucket: s3.IBucket
    athenaResultsBucket: s3.IBucket
}

export class genericApiStack extends cdk.Stack {
    constructor (
        scope: Construct,
        id: string,
        stageConfig: StageConfig,
        props: genericApiStackProps,
      ) {
        super(scope, id, props)

        const apiLambdaName = `generic-${stageConfig.stage}-api-lambda`

        const apiImage = new ecrAssets.DockerImageAsset(
            this,
            'ApiImageAsset',
            {
              directory: path.join(
                __dirname,
                '../../lambda/api-lambda',
              ),
              invalidation: {
                buildArgs: false,
              },
            },
          )

        const apiLambda = new lambda.DockerImageFunction(this, 'ApiLambda', {
            functionName: apiLambdaName,
            code: lambda.DockerImageCode.fromEcr(apiImage.repository, { tag: apiImage.imageTag }),
            memorySize: 256,
            architecture: lambda.Architecture.ARM_64,
            environment: {
                DATA_LAKE_BUCKET_NAME: props.dataLakeBucket.bucketName,
                DATABASE: 'dev-generic-data-catalog',
                OUTPUT_LOCATION: 's3://generic-dev-athena-results/',
                BATCH_SIZE: '1000',
            },
            timeout: cdk.Duration.minutes(15),
        });

        props.dataLakeBucket.grantReadWrite(apiLambda)
        props.athenaResultsBucket.grantReadWrite(apiLambda)

        // Grant Athena permissions to the Lambda function
        const athenaPolicy = new iam.PolicyStatement({
            actions: [
                'athena:StartQueryExecution',
                'athena:GetQueryExecution',
                'athena:GetQueryResults'
            ],
            resources: [
                `arn:aws:athena:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:workgroup/primary`
            ]
        });

        const gluePolicy = new iam.PolicyStatement({
            actions: [
                'glue:GetDatabase',
                'glue:GetTable',
                'glue:GetPartition',
                'glue:GetPartitions'
            ],
            resources: [
                `arn:aws:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:catalog`,
                `arn:aws:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:database/*`,
                `arn:aws:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:table/*/*`
            ]
        });

        apiLambda.addToRolePolicy(gluePolicy);

        apiLambda.addToRolePolicy(athenaPolicy);

        // Create an API Gateway REST API
        const api = new apigateway.RestApi(this, 'ApiGateway', {
            restApiName: `GenericApi-${stageConfig.stage}`,
            description: `API Gateway for the Generic API Lambda Function in ${stageConfig.stage} stage`,
        });

        // Integrate the API Gateway with the Lambda function
        const integration = new apigateway.LambdaIntegration(apiLambda);

        // Define a root resource and a GET method for the API
        api.root.addMethod('GET', integration);

        const uploadResource = api.root.addResource('upload');
        uploadResource.addMethod('POST', integration);

        const employeeHiresResource = api.root.addResource('employee-hires');
        employeeHiresResource.addMethod('GET', integration);

        const overMeanEmployeesResource = api.root.addResource('over-mean-employee-hires');
        overMeanEmployeesResource.addMethod('GET', integration);

      }
}