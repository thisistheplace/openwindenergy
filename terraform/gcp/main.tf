terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.8.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = "us-east1"
  zone    = "us-east1-c"
}

resource "google_compute_network" "vpc_network" {
  name = "terraform-network"
}

resource "google_compute_instance" "vm_instance" {
  name         = "terraform-instance"
  machine_type = "c4a-highmem-1"
  tags         = ["ssh", "http-server", "https-server"]
  metadata_startup_script = file("../../openwindenergy-build-ubuntu.sh")

  boot_disk {
    initialize_params {
      image = "ubuntu-2504-plucky-arm64-v20250424"
      size = "120"
      type = "pd-ssd"
    }
  }

  environment {
    variables = {
      SERVER_USERNAME = var.username
      SERVER_PASSWORD = var.password
    }
  }

  network_interface {
    network = google_compute_network.vpc_network.name
    access_config {
    }
  }
}

resource "google_compute_firewall" "allow_ssh" {
  name        = "allow-ssh"
  network     = google_compute_network.vpc_network.name
  direction   = "INGRESS"
  priority    = 1000
  target_tags = ["ssh"] # Replace with your instance's target tag
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
  source_ranges = ["0.0.0.0/0"] # Allow from any IP (for testing, restrict in production)
}

resource "google_compute_firewall" "allow_http_https" {
  name          = "allow-http-https"
  network       = google_compute_network.vpc_network.name
  priority      = 1000          
  direction     = "INGRESS"
  source_ranges = ["0.0.0.0/0"]
  allow {
    protocol = "tcp"
    ports    = ["80", "443", "9001"]
  }
  target_tags = ["http-server", "https-server"]
  description = "Allow HTTP and HTTPS traffic"
}
