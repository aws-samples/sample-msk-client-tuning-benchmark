// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
import * as cdk from 'aws-cdk-lib/core';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { CfnOutput } from 'aws-cdk-lib/core';

export interface NetworkStackProps extends cdk.StackProps {
  /** CIDR block for the VPC. */
  readonly cidr: string;
}

/**
 * VPC with public + 3 private (egress) subnets across 3 AZs (MSK requires
 * broker subnets in at least 2, and 3 gives an even RF=3 spread), a client
 * security group for the producer/consumer hosts, and an MSK broker security
 * group. The MSK cluster is created out-of-band by create-msk-standard-cluster.sh
 * (not by AWS CloudFormation), so the broker SG ingress is opened to the client
 * SG ahead of the cluster's existence. No security group allows ingress from
 * 0.0.0.0/0; the hosts are reached only through AWS Systems Manager.
 */
export class NetworkStack extends cdk.Stack {
  public readonly vpc: ec2.Vpc;
  public readonly mskSecurityGroup: ec2.SecurityGroup;
  public readonly clientSecurityGroup: ec2.SecurityGroup;

  constructor(scope: Construct, id: string, props: NetworkStackProps) {
    super(scope, id, props);

    const region = props.env?.region as string;
    this.vpc = new ec2.Vpc(this, 'Vpc', {
      ipAddresses: ec2.IpAddresses.cidr(props.cidr),
      availabilityZones: [`${region}a`, `${region}b`, `${region}c`],
      // A single NAT gateway is a potential egress bottleneck for a fleet that
      // pulls hundreds of MB/s. Client<->broker traffic stays INSIDE the VPC
      // (private subnets, no NAT), so the NAT only carries package installs,
      // S3 (via NAT here for simplicity), SSM, and CloudWatch. That is modest,
      // so one NAT gateway is fine and keeps cost down.
      natGateways: 1,
      subnetConfiguration: [
        { name: 'public', subnetType: ec2.SubnetType.PUBLIC, cidrMask: 24 },
        { name: 'private', subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS, cidrMask: 24 },
      ],
    });

    this.clientSecurityGroup = new ec2.SecurityGroup(this, 'ClientSg', {
      vpc: this.vpc,
      description: 'MSK client SG (producer + consumer fleet)',
      allowAllOutbound: true,
    });

    this.mskSecurityGroup = new ec2.SecurityGroup(this, 'MskSg', {
      vpc: this.vpc,
      description: 'MSK broker SG (load benchmark)',
      allowAllOutbound: true,
    });

    // Allow client hosts to reach brokers on the IAM-authenticated SASL/TLS
    // listener only (9098). create-msk-standard-cluster.sh enables IAM auth
    // with TLS in transit and no other client authentication, so the
    // plaintext (9092), TLS (9094), and SASL/SCRAM (9096) listeners are not
    // opened. Add them here only if you enable those auth modes.
    this.mskSecurityGroup.addIngressRule(
      this.clientSecurityGroup,
      ec2.Port.tcp(9098),
      'Kafka IAM auth (SASL/TLS) from client hosts',
    );

    new CfnOutput(this, 'VpcId', { value: this.vpc.vpcId });
    new CfnOutput(this, 'PrivateSubnetIds', {
      value: this.vpc.privateSubnets.map((s) => s.subnetId).join(','),
    });
    new CfnOutput(this, 'MskSecurityGroupId', { value: this.mskSecurityGroup.securityGroupId });
    new CfnOutput(this, 'ClientSecurityGroupId', { value: this.clientSecurityGroup.securityGroupId });
  }
}
