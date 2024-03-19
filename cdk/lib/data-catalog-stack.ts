import * as cdk from 'aws-cdk-lib'
import * as glue from 'aws-cdk-lib/aws-glue'
import * as athena from 'aws-cdk-lib/aws-athena'
import { type Construct } from 'constructs'
import { type DataStack } from './data-stack'
import { type StageConfig } from './config-builder'
import * as cr from 'aws-cdk-lib/custom-resources'
import * as fs from 'fs'
import * as path from 'path'

interface DataCatalogStackProps extends cdk.StackProps {
    dataStack: DataStack
  }

  export class DataCatalogStack extends cdk.Stack {

    constructor (
        scope: Construct,
        id: string,
        stageConfig: StageConfig,
        props: DataCatalogStackProps,
      ) {
        super(scope, id, props)

        const dataCatalogName = `${stageConfig.stage}-generic-data-catalog`
        const dataLakeBucket = props.dataStack.dataLakeBucket
        const athenaResultsBucket = props.dataStack.athenaResultsBucket

        const genericDataCatalog = new glue.CfnDatabase(
            this,
            'genericDataCatalog',
            {
              catalogId: cdk.Aws.ACCOUNT_ID,
              databaseInput: {
                name: dataCatalogName,
                description: 'Database for the generic data lake',
                locationUri: `s3://${dataLakeBucket.bucketName}/`,
              },
            },
          )

          const hiredEmployeesTable = new glue.CfnTable(this, 'HiredEmployeesTable', {
            catalogId: cdk.Aws.ACCOUNT_ID,
            databaseName: dataCatalogName,
            tableInput: {
                name: 'hired_employees',
                storageDescriptor: {
                    columns: [
                        { name: 'id', type: 'int' },
                        { name: 'name', type: 'string' },
                        { name: 'datetime', type: 'string' },
                        { name: 'department_id', type: 'int' },
                        { name: 'job_id', type: 'int' }
                    ],
                    location: `s3://${dataLakeBucket.bucketName}/hired_employees/`,
                    inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                    outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    serdeInfo: {
                        serializationLibrary: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                        parameters: {
                            'field.delim': ',',
                            'serialization.format': ','
                        }
                    },
                    compressed: true
                },
                tableType: 'EXTERNAL_TABLE',
            },
        });

        // Define the departments table for CSV GZIP files
        const departmentsTable = new glue.CfnTable(this, 'DepartmentsTable', {
            catalogId: cdk.Aws.ACCOUNT_ID,
            databaseName: dataCatalogName,
            tableInput: {
                name: 'departments',
                storageDescriptor: {
                    columns: [
                        { name: 'id', type: 'int' },
                        { name: 'department', type: 'string' }
                    ],
                    location: `s3://${dataLakeBucket.bucketName}/departments/`,
                    inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                    outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    serdeInfo: {
                        serializationLibrary: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                        parameters: {
                            'field.delim': ',',
                            'serialization.format': ','
                        }
                    },
                    compressed: true
                },
                tableType: 'EXTERNAL_TABLE',
            },
        });

        // Define the jobs table for CSV GZIP files
        const jobsTable = new glue.CfnTable(this, 'JobsTable', {
            catalogId: cdk.Aws.ACCOUNT_ID,
            databaseName: dataCatalogName,
            tableInput: {
                name: 'jobs',
                storageDescriptor: {
                    columns: [
                        { name: 'id', type: 'int' },
                        { name: 'job', type: 'string' }
                    ],
                    location: `s3://${dataLakeBucket.bucketName}/jobs/`,
                    inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                    outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    serdeInfo: {
                        serializationLibrary: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                        parameters: {
                            'field.delim': ',',
                            'serialization.format': ','
                        }
                    },
                    compressed: true
                },
                tableType: 'EXTERNAL_TABLE',
            },
        });

        hiredEmployeesTable.addDependency(genericDataCatalog)
        departmentsTable.addDependency(genericDataCatalog)
        jobsTable.addDependency(genericDataCatalog)

      }
  }