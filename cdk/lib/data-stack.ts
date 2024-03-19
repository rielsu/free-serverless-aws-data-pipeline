import * as cdk from 'aws-cdk-lib'
import { type Construct } from 'constructs'
import * as s3 from 'aws-cdk-lib/aws-s3'
import { type StageConfig } from './config-builder'

interface DataStackProps extends cdk.StackProps {}

export class DataStack extends cdk.Stack{

    public readonly dataLakeBucket: s3.Bucket
    public readonly athenaResultsBucket: s3.Bucket

    constructor (
        scope: Construct,
        id: string,
        stageConfig: StageConfig,
        props: DataStackProps,
      ) {
        super(scope, id, props)

    const dataLakeBucketName = `generic-${stageConfig.stage}-data-lake`
    const athenaResultsBucketName = `generic-${stageConfig.stage}-athena-results`

    this.dataLakeBucket = new s3.Bucket(this, 'DataLakeBucket', {
        bucketName: dataLakeBucketName,
        versioned: true,
        encryption: s3.BucketEncryption.S3_MANAGED,
        blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
        enforceSSL: true,
        lifecycleRules: [
          {
            expiration: cdk.Duration.days(90),
            transitions: [
              {
                storageClass: s3.StorageClass.GLACIER,
                transitionAfter: cdk.Duration.days(60),
              },
            ],
          },
        ],
        objectOwnership: s3.ObjectOwnership.BUCKET_OWNER_PREFERRED,
        removalPolicy:
          stageConfig.stage === 'dev'
            ? cdk.RemovalPolicy.DESTROY
            : cdk.RemovalPolicy.RETAIN,
        autoDeleteObjects: stageConfig.stage === 'dev',
        cors: [
          {
            allowedMethods: [s3.HttpMethods.GET],
            allowedOrigins: ['*'],
          },
        ],
      })

    this.athenaResultsBucket = new s3.Bucket(this, 'AthenaResultsBucket', {
    bucketName: athenaResultsBucketName,
    removalPolicy: cdk.RemovalPolicy.DESTROY,
    autoDeleteObjects: true,
    })

}
}