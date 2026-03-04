#############################################
# MSK Serverless (Kafka) - UrbanFlow (DEV)
# Auth: IAM (SASL/IAM) - porta 9098
#############################################

# Default VPC (rápido pra DEV). Depois podemos apontar pra VPC do Fraud.
data "aws_vpc" "default" {
  default = true
}

# Lista subnets disponíveis na VPC
data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }

  filter {
    name   = "state"
    values = ["available"]
  }
}

# Detalhes de cada subnet (inclui AZ)
data "aws_subnet" "by_id" {
  for_each = toset(data.aws_subnets.default.ids)
  id       = each.value
}

locals {
  # Agrupa subnets por Availability Zone
  # Ex.: { "us-east-1a" = ["subnet-aaa","subnet-bbb"], "us-east-1b" = ["subnet-ccc"] }
  az_to_subnets = {
    for s in values(data.aws_subnet.by_id) :
    s.availability_zone => s.id...
  }

  # Seleciona AZs disponíveis e pega 2 primeiras (ordenadas)
  selected_azs = slice(sort(keys(local.az_to_subnets)), 0, 2)

  # Escolhe 1 subnet por AZ (primeira da lista)
  msk_subnet_ids = [for az in local.selected_azs : local.az_to_subnets[az][0]]
}

resource "aws_security_group" "msk_serverless_sg" {
  name        = "${var.project}-${var.environment}-msk-serverless-sg"
  description = "Security group for MSK Serverless (SASL/IAM 9098)"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "Kafka SASL/IAM from within VPC"
    from_port   = 9098
    to_port     = 9098
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.default.cidr_block]
  }

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Project     = "urbanflow-data-platform"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

resource "aws_msk_serverless_cluster" "urbanflow" {
  cluster_name = "${var.project}-${var.environment}-msk-serverless"

  vpc_config {
    subnet_ids         = local.msk_subnet_ids
    security_group_ids = [aws_security_group.msk_serverless_sg.id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }

  tags = {
    Project     = "urbanflow-data-platform"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}
