// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
import * as cdk from 'aws-cdk-lib/core';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { NetworkStack } from '../lib/network-stack';
import { ClientHostStack } from '../lib/client-host-stack';

const env = { account: '111122223333', region: 'us-east-1' };

function build() {
  const app = new cdk.App();
  const network = new NetworkStack(app, 'Net', { env, cidr: '10.60.0.0/16' });
  const host = new ClientHostStack(app, 'Host', {
    env,
    vpc: network.vpc,
    clientSecurityGroup: network.clientSecurityGroup,
    clusterNames: ['load-benchmark'],
    instanceType: new ec2.InstanceType('c5n.2xlarge'),
  });
  return { net: Template.fromStack(network), host: Template.fromStack(host) };
}

test('broker SG has no 0.0.0.0/0 ingress and only the IAM listener', () => {
  const { net } = build();
  const ingress = net.findResources('AWS::EC2::SecurityGroupIngress');
  for (const r of Object.values(ingress)) {
    expect(r.Properties.CidrIp).toBeUndefined();
    expect(r.Properties.FromPort).toBe(9098);
  }
});

test('hosts require IMDSv2 and use encrypted EBS', () => {
  const { host } = build();
  host.hasResourceProperties('AWS::EC2::LaunchTemplate', {
    LaunchTemplateData: { MetadataOptions: { HttpTokens: 'required' } },
  });
  host.hasResourceProperties('AWS::EC2::Instance', {
    BlockDeviceMappings: Match.arrayWith([
      Match.objectLike({ Ebs: Match.objectLike({ Encrypted: true }) }),
    ]),
  });
});

test('scripts bucket blocks public access and enforces TLS', () => {
  const { host } = build();
  host.hasResourceProperties('AWS::S3::Bucket', {
    PublicAccessBlockConfiguration: {
      BlockPublicAcls: true,
      BlockPublicPolicy: true,
      IgnorePublicAcls: true,
      RestrictPublicBuckets: true,
    },
  });
  host.hasResourceProperties('AWS::S3::BucketPolicy', {
    PolicyDocument: {
      Statement: Match.arrayWith([
        Match.objectLike({
          Effect: 'Deny',
          Condition: { Bool: { 'aws:SecureTransport': 'false' } },
        }),
      ]),
    },
  });
});
