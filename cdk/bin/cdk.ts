#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { type StageConfig, getConfig } from '../lib/config-builder'
import { CdkStack } from '../lib/cdk-stack';
import { DataStack } from '../lib/data-stack'
import { genericApiStack } from '../lib/api-stack'
import { DataCatalogStack } from '../lib/data-catalog-stack'

const app = new cdk.App();

const stageConfig: StageConfig = getConfig(app)

const dataLakeStack = new DataStack(app, 'DataStack', stageConfig, {
  env: {
    account: stageConfig.awsAccountId,
    region: stageConfig.awsRegion,
  },
})

const apiStack = new genericApiStack(app, 'ApiStack', stageConfig, {
    dataLakeBucket: dataLakeStack.dataLakeBucket,
    athenaResultsBucket: dataLakeStack.athenaResultsBucket,
    env: {
      account: stageConfig.awsAccountId,
      region: stageConfig.awsRegion,
    },
})

const dataCatalogStack = new DataCatalogStack(app, 'DataCatalogStack', stageConfig, {
  dataStack: dataLakeStack,
  env: {
    account: stageConfig.awsAccountId,
    region: stageConfig.awsRegion,
  },
})


apiStack.addDependency(dataLakeStack)
dataCatalogStack.addDependency(dataLakeStack)

