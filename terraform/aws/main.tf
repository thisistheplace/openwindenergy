terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }

  required_version = ">= 1.2.0"
}

provider "aws" {
  region = "us-east-1"
}

resource "aws_instance" "openwindenergy-server" {
  ami           = "ami-04542995864e26699"
  instance_type = "t3.micro"
  security_groups = ["${aws_security_group.ingress-all-test.id}"]
  subnet_id = "${aws_subnet.subnet-uno.id}"
  user_data = file("../../openwindenergy-build-ubuntu.sh")

  tags = {
    Name = "openwindenergy-server"
  }

  root_block_device {
    volume_size = 120
    volume_type = "gp3"
    encrypted   = false
  }
}

resource "aws_vpc" "openwindenergy-env" {
  cidr_block = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support = true
  tags = {
    Name = "openwindenergy-env"
  }
}

resource "aws_eip" "ip-openwindenergy-env" {
  instance = "${aws_instance.openwindenergy-server.id}"
  vpc      = true
}

resource "aws_internet_gateway" "openwindenergy-env-gw" {
  vpc_id = "${aws_vpc.openwindenergy-env.id}"
  tags = {
    Name = "openwindenergy-env-gw"
  }
}

resource "aws_subnet" "subnet-uno" {
  cidr_block = "${cidrsubnet(aws_vpc.openwindenergy-env.cidr_block, 3, 1)}"
  vpc_id = "${aws_vpc.openwindenergy-env.id}"
  availability_zone = "us-east-1a"
}

resource "aws_route_table" "route-table-openwindenergy-env" {
  vpc_id = "${aws_vpc.openwindenergy-env.id}"
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = "${aws_internet_gateway.openwindenergy-env-gw.id}"
  }
  tags = {
    Name = "openwindenergy-env-route-table"
  }
}
resource "aws_route_table_association" "subnet-association" {
  subnet_id      = "${aws_subnet.subnet-uno.id}"
  route_table_id = "${aws_route_table.route-table-openwindenergy-env.id}"
}

resource "aws_security_group" "ingress-all-test" {
  name = "allow-all-sg"
  description = "Allow SSH, HTTP and HTTPS"
  vpc_id = "${aws_vpc.openwindenergy-env.id}"
  ingress {
    cidr_blocks = [
      "0.0.0.0/0"
    ]
    from_port = 22
    to_port = 22
    protocol = "tcp"
  }

  ingress {
    cidr_blocks = [
      "0.0.0.0/0"
    ]
    from_port = 80
    to_port = 80
    protocol = "tcp"
  }

  ingress {
    cidr_blocks = [
      "0.0.0.0/0"
    ]
    from_port = 443
    to_port = 443
    protocol = "tcp"
  }

  // Terraform removes the default rule
  egress {
   from_port = 0
   to_port = 0
   protocol = "-1"
   cidr_blocks = ["0.0.0.0/0"]
 }
}
