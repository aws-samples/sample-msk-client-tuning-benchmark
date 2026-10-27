// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
import * as cdk from 'aws-cdk-lib/core';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { CfnOutput } from 'aws-cdk-lib/core';

// Pinned checksums for the client-host downloads in user data. Update them together
// with the version numbers in the URLs below.
const KAFKA_TGZ_SHA512 =
  '01186a5086b5ae753811ee71808a797baec1303d4e0d4b1c0acf42c489654a7b8988136f8469d0b66f0be439f8e3e9b816171ea0b0d7a36462b07aa2d3ef0023'; // pragma: allowlist secret - public SHA-512 checksum, not a secret
const MSK_IAM_AUTH_JAR_SHA256 = '735dd058b81d088e767770d6cc4e7f1a1beea815ced18b66a23b35fd87651e77'; // pragma: allowlist secret - public SHA-256 checksum, not a secret

export interface ClientHostStackProps extends cdk.StackProps {
  readonly vpc: ec2.IVpc;
  readonly clientSecurityGroup: ec2.ISecurityGroup;
  /**
   * Name(s) the MSK cluster(s) will be created with via create-msk-standard-cluster.sh.
   * The cluster does not exist at CDK deploy time (it is created out-of-band),
   * so the host's data-plane IAM is scoped by cluster NAME rather than ARN.
   */
  readonly clusterNames: string[];
  /**
   * EC2 instance type for this host. Producer/consumer hosts default to
   * c5n.2xlarge (network-optimized, ~10 Gbps guaranteed, 8 vCPU for TLS + IAM
   * signing) which comfortably drives ~180 MB/s from a single host. For other
   * throughput targets, override the size with CDK context (see "Configuration"
   * in infra/README.md).
   */
  readonly instanceType: ec2.InstanceType;
  /**
   * How many identical hosts to create in this stack. The producer stack uses
   * 1; the consumer stack sets this to `consumerCount` to build a fleet, each
   * host running its own consumer group. Every host gets a numbered logical id
   * and its instance id is emitted as `HostInstanceId` (host 0) plus
   * `HostInstanceId<N>` for the rest.
   */
  readonly hostCount?: number;
  /** Root volume size in GiB. See the comment on the block device below. */
  readonly rootVolumeSizeGiB?: number;
}

/**
 * A pool of identical MSK client hosts (producers or consumers) with the Kafka
 * CLI + perf-test tools and IAM auth pre-installed via user data, plus an S3
 * bucket used to stage scripts onto the hosts and pull results back off (see
 * orchestration/run-on-host.sh). IAM permissions are scoped by cluster name
 * (not ARN, since the cluster does not exist at deploy time).
 *
 * The same stack class provisions both roles; the only difference is the
 * instance type, the host count, and which host-side script you run on it.
 * Keeping producers and consumers on separate hosts is deliberate: at ~180
 * MB/s each, sharing one host would make the client the network bottleneck and
 * invalidate the broker measurements.
 */
export class ClientHostStack extends cdk.Stack {
  public readonly instances: ec2.Instance[] = [];
  public readonly scriptsBucket: s3.Bucket;

  constructor(scope: Construct, id: string, props: ClientHostStackProps) {
    super(scope, id, props);

    this.scriptsBucket = new s3.Bucket(this, 'ScriptsBucket', {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      // Deny any non-TLS request to the bucket.
      enforceSSL: true,
      objectOwnership: s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    const role = new iam.Role(this, 'ClientRole', {
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
      managedPolicies: [
        // Enables SSM RunShellScript: no SSH key, no inbound rules needed.
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore'),
      ],
    });
    this.scriptsBucket.grantReadWrite(role);

    const clusterResources = props.clusterNames.map(
      (name) => `arn:${this.partition}:kafka:${this.region}:${this.account}:cluster/${name}/*`,
    );
    const topicResources = props.clusterNames.map(
      (name) => `arn:${this.partition}:kafka:${this.region}:${this.account}:topic/${name}/*`,
    );
    const groupResources = props.clusterNames.map(
      (name) => `arn:${this.partition}:kafka:${this.region}:${this.account}:group/${name}/*`,
    );
    const transactionalIdResources = props.clusterNames.map(
      (name) => `arn:${this.partition}:kafka:${this.region}:${this.account}:transactional-id/${name}/*`,
    );
    const configurationResource = `arn:${this.partition}:kafka:${this.region}:${this.account}:configuration/*`;

    // Data-plane: connect, describe, produce, consume, manage topics + groups.
    role.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          'kafka-cluster:Connect',
          'kafka-cluster:DescribeCluster',
          'kafka-cluster:CreateTopic',
          'kafka-cluster:DescribeTopic',
          'kafka-cluster:AlterTopic',
          'kafka-cluster:DescribeTopicDynamicConfiguration',
          'kafka-cluster:AlterTopicDynamicConfiguration',
          'kafka-cluster:DeleteTopic',
          'kafka-cluster:WriteData',
          'kafka-cluster:ReadData',
          'kafka-cluster:WriteDataIdempotently',
          'kafka-cluster:AlterGroup',
          'kafka-cluster:DescribeGroup',
          'kafka-cluster:DescribeTransactionalId',
          'kafka-cluster:AlterTransactionalId',
        ],
        resources: [...clusterResources, ...topicResources, ...groupResources, ...transactionalIdResources],
      }),
    );

    // Control-plane reads scoped to the benchmark cluster(s): bootstrap
    // brokers, cluster description, and node list.
    role.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          'kafka:GetBootstrapBrokers',
          'kafka:DescribeCluster',
          'kafka:DescribeClusterV2',
          'kafka:ListNodes',
        ],
        resources: clusterResources,
      }),
    );
    role.addToPolicy(
      new iam.PolicyStatement({
        actions: ['kafka:DescribeConfiguration', 'kafka:DescribeConfigurationRevision'],
        resources: [configurationResource],
      }),
    );

    // Read-only list/metric calls. These APIs do not support resource-level
    // permissions, so the resource must be '*'.
    role.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          'kafka:ListClusters',
          'kafka:ListClustersV2',
          'kafka:ListConfigurations',
          'cloudwatch:GetMetricData',
          'cloudwatch:GetMetricStatistics',
        ],
        resources: ['*'],
      }),
    );

    // scripts/15-put-dashboard.sh can be run from a host; dashboards are
    // account-global resources (no region in the ARN).
    role.addToPolicy(
      new iam.PolicyStatement({
        actions: ['cloudwatch:PutDashboard', 'cloudwatch:GetDashboard'],
        resources: [`arn:${this.partition}:cloudwatch::${this.account}:dashboard/*`],
      }),
    );

    const userData = ec2.UserData.forLinux();
    userData.addCommands(
      'set -eux',
      'dnf install -y java-17-amazon-corretto-headless jq python3 bc logrotate cronie',
      'cd /opt',
      // Downloads are pinned and verified; the host bootstrap fails (set -e) on a mismatch.
      // Kafka: sha512 from archive.apache.org/dist/kafka/3.9.2/kafka_2.13-3.9.2.tgz.sha512.
      'curl -fsSL --retry 5 -o kafka.tgz https://archive.apache.org/dist/kafka/3.9.2/kafka_2.13-3.9.2.tgz',
      `echo "${KAFKA_TGZ_SHA512}  kafka.tgz" | sha512sum -c -`,
      'tar xzf kafka.tgz',
      'ln -s /opt/kafka_2.13-3.9.2 /opt/kafka',
      // aws-msk-iam-auth: sha256 from the .sha256 file published with the GitHub release.
      'curl -fsSL --retry 5 -o /opt/kafka/libs/aws-msk-iam-auth.jar ' +
        'https://github.com/aws/aws-msk-iam-auth/releases/download/v2.2.0/aws-msk-iam-auth-2.2.0-all.jar',
      `echo "${MSK_IAM_AUTH_JAR_SHA256}  /opt/kafka/libs/aws-msk-iam-auth.jar" | sha256sum -c -`,
      // Mandatory IAM auth config. Load scripts pass this AND the tunable
      // client-tuning.properties to the perf tools, so this file only carries
      // the auth bits that must always be present.
      'cat > /opt/kafka/config/client-iam.properties <<EOF\n' +
        'security.protocol=SASL_SSL\n' +
        'sasl.mechanism=AWS_MSK_IAM\n' +
        'sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;\n' +
        'sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler\n' +
        'EOF',
      'echo \'export PATH=/opt/kafka/bin:$PATH\' >> /etc/profile.d/kafka.sh',
      'mkdir -p /opt/load-benchmark/scripts /opt/load-benchmark/results',
      'chmod -R 755 /opt/load-benchmark',
      // Cap load-generator log growth. copytruncate is required because the
      // load scripts hold the logs open via `>` redirect for days -- a plain
      // rename would leave each process writing to an unlinked inode, so the
      // space would never actually be reclaimed. Driven hourly rather than
      // daily, since a day of churn at this throughput fills the disk.
      'cat > /etc/logrotate.d/load-benchmark <<EOF\n' +
        '/opt/load-benchmark/results/*.log {\n' +
        '  size 200M\n' +
        '  rotate 3\n' +
        '  compress\n' +
        '  copytruncate\n' +
        '  missingok\n' +
        '  notifempty\n' +
        '}\n' +
        'EOF',
      'cat > /etc/cron.hourly/logrotate-load-benchmark <<EOF\n' +
        '#!/bin/bash\n' +
        '/usr/sbin/logrotate /etc/logrotate.d/load-benchmark\n' +
        'EOF',
      'chmod +x /etc/cron.hourly/logrotate-load-benchmark',
      'systemctl enable --now crond',
    );

    const machineImage = ec2.MachineImage.fromSsmParameter(
      '/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64',
      { os: ec2.OperatingSystemType.LINUX },
    );

    const count = props.hostCount ?? 1;
    for (let i = 0; i < count; i++) {
      const instance = new ec2.Instance(this, count === 1 ? 'Host' : `Host${i}`, {
        vpc: props.vpc,
        vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
        instanceType: props.instanceType,
        machineImage,
        securityGroup: props.clientSecurityGroup,
        role,
        userData,
        userDataCausesReplacement: false,
        // IMDSv2 only (session tokens required for instance metadata).
        requireImdsv2: true,
        blockDevices: [
          {
            deviceName: '/dev/xvda',
            // The AMI default is 8 GiB, which is far too small: the perf-test
            // load generators write one log per topic per role and get very
            // chatty under sustained multi-hundred-MB/s load. A full root
            // volume takes out both the load generators AND the SSM agent, and
            // cannot be recovered by growing the EBS volume + rebooting
            // (cloud-init's growpart fails on "No space left on device"). Give
            // it real headroom.
            volume: ec2.BlockDeviceVolume.ebs(props.rootVolumeSizeGiB ?? 100, {
              volumeType: ec2.EbsDeviceVolumeType.GP3,
              encrypted: true,
              deleteOnTermination: true,
            }),
          },
        ],
      });
      this.instances.push(instance);

      const outputSuffix = i === 0 ? '' : String(i);
      new CfnOutput(this, `HostInstanceId${outputSuffix}`, {
        value: instance.instanceId,
      });
    }

    new CfnOutput(this, 'ScriptsBucketName', { value: this.scriptsBucket.bucketName });
    new CfnOutput(this, 'HostCount', { value: String(count) });
  }
}
