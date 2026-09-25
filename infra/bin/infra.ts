#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib/core';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { NetworkStack } from '../lib/network-stack';
import { ClientHostStack } from '../lib/client-host-stack';

const app = new cdk.App();

// ---------------------------------------------------------------------------
// Account / region. Override without editing this file:
//   npx cdk deploy --all -c account=123456789012 -c region=us-west-2
// or set CDK_DEFAULT_ACCOUNT / CDK_DEFAULT_REGION in the environment.
// ---------------------------------------------------------------------------
const REGION =
  app.node.tryGetContext('region') ?? process.env.CDK_DEFAULT_REGION ?? 'us-east-1';
const ACCOUNT =
  app.node.tryGetContext('account') ?? process.env.CDK_DEFAULT_ACCOUNT;
const env = ACCOUNT ? { account: ACCOUNT, region: REGION } : { region: REGION };

// The name the Standard MSK cluster is created with by
// create-msk-standard-cluster.sh. The client hosts' data-plane IAM is scoped
// to this name ahead of the cluster's existence, so it MUST match the
// CLUSTER_NAME in that script exactly.
const CLUSTER_NAME = app.node.tryGetContext('clusterName') ?? 'load-benchmark';

// How many consumer hosts to build. Each host runs one consumer group reading
// the full topic set, so raising this multiplies cluster egress (fan-out).
// Default 1 = one group consuming the ~180 MB/s the producers generate.
//   npx cdk deploy --all -c consumerCount=3
const CONSUMER_COUNT = Number(app.node.tryGetContext('consumerCount') ?? 1);

// Instance types. c5n is network-optimized with GUARANTEED (not burst)
// bandwidth, which is what a multi-hour high-throughput soak needs. See the
// sizing table in the root README to pick sizes for other targets.
//   -c producerInstanceType=c5n.4xlarge -c consumerInstanceType=c5n.4xlarge
const PRODUCER_INSTANCE_TYPE = app.node.tryGetContext('producerInstanceType') ?? 'c5n.2xlarge';
const CONSUMER_INSTANCE_TYPE = app.node.tryGetContext('consumerInstanceType') ?? 'c5n.2xlarge';

const network = new NetworkStack(app, 'LoadBenchmarkNetworkStack', {
  env,
  cidr: app.node.tryGetContext('cidr') ?? '10.60.0.0/16',
});

// One producer host, sized to drive ~180 MB/s from a single instance.
const producer = new ClientHostStack(app, 'LoadBenchmarkProducerStack', {
  env,
  vpc: network.vpc,
  clientSecurityGroup: network.clientSecurityGroup,
  clusterNames: [CLUSTER_NAME],
  instanceType: new ec2.InstanceType(PRODUCER_INSTANCE_TYPE),
  hostCount: 1,
});
producer.addDependency(network);

// A fleet of consumer hosts, one stack per host so each can be created,
// destroyed, and driven independently (spin off as many as you want). Each
// host is scoped to the same cluster name and runs its own consumer group.
for (let i = 0; i < CONSUMER_COUNT; i++) {
  const consumer = new ClientHostStack(app, `LoadBenchmarkConsumerStack-${i}`, {
    env,
    vpc: network.vpc,
    clientSecurityGroup: network.clientSecurityGroup,
    clusterNames: [CLUSTER_NAME],
    instanceType: new ec2.InstanceType(CONSUMER_INSTANCE_TYPE),
    hostCount: 1,
  });
  consumer.addDependency(network);
}
