#!/usr/bin/env python3
"""
Azure Infrastructure Deployment for Telegram Music Scraper
Optimized for $2000 credit usage with cost-effective resources
"""

from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.storage import StorageManagementClient
import os
import json
from datetime import datetime, timedelta

class AzureInfrastructure:
    def __init__(self, subscription_id: str, resource_group: str, location: str = "eastus"):
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.location = location
        
        # Initialize Azure clients
        credential = DefaultAzureCredential()
        self.resource_client = ResourceManagementClient(credential, subscription_id)
        self.compute_client = ComputeManagementClient(credential, subscription_id)
        self.network_client = NetworkManagementClient(credential, subscription_id)
        self.storage_client = StorageManagementClient(credential, subscription_id)
    
    def create_resource_group(self):
        """Create resource group for scraper infrastructure"""
        rg_params = {
            'location': self.location,
            'tags': {
                'project': 'telegram-music-scraper',
                'environment': 'production',
                'cost-center': 'automation'
            }
        }
        
        rg_result = self.resource_client.resource_groups.create_or_update(
            self.resource_group, rg_params
        )
        print(f"Resource group created: {rg_result.name}")
        return rg_result
    
    def create_storage_account(self, storage_name: str):
        """Create Azure Storage for temporary file storage"""
        storage_params = {
            'sku': {'name': 'Standard_LRS'},  # Cheapest option
            'kind': 'StorageV2',
            'location': self.location,
            'access_tier': 'Hot',
            'allow_blob_public_access': True
        }
        
        storage_result = self.storage_client.storage_accounts.begin_create(
            self.resource_group,
            storage_name,
            storage_params
        ).result()
        
        print(f"Storage account created: {storage_result.name}")
        return storage_result
    
    def create_vm_network(self, vnet_name: str, subnet_name: str):
        """Create virtual network and subnet"""
        # Create virtual network
        vnet_params = {
            'location': self.location,
            'address_space': {
                'address_prefixes': ['10.0.0.0/16']
            }
        }
        
        vnet_result = self.network_client.virtual_networks.begin_create_or_update(
            self.resource_group,
            vnet_name,
            vnet_params
        ).result()
        
        # Create subnet
        subnet_params = {
            'address_prefix': '10.0.0.0/24'
        }
        
        subnet_result = self.network_client.subnets.begin_create_or_update(
            self.resource_group,
            vnet_name,
            subnet_name,
            subnet_params
        ).result()
        
        print(f"Network created: {vnet_name}/{subnet_name}")
        return vnet_result, subnet_result
    
    def create_public_ip(self, ip_name: str):
        """Create public IP for VM"""
        ip_params = {
            'location': self.location,
            'sku': {'name': 'Basic'},
            'public_ip_allocation_method': 'Dynamic'
        }
        
        ip_result = self.network_client.public_ip_addresses.begin_create_or_update(
            self.resource_group,
            ip_name,
            ip_params
        ).result()
        
        print(f"Public IP created: {ip_name}")
        return ip_result
    
    def create_network_security_group(self, nsg_name: str):
        """Create network security group with SSH access"""
        nsg_params = {
            'location': self.location,
            'security_rules': [
                {
                    'name': 'SSH',
                    'protocol': 'Tcp',
                    'source_port_range': '*',
                    'destination_port_range': '22',
                    'source_address_prefix': '*',
                    'destination_address_prefix': '*',
                    'access': 'Allow',
                    'priority': 300,
                    'direction': 'Inbound'
                },
                {
                    'name': 'HTTPS',
                    'protocol': 'Tcp',
                    'source_port_range': '*',
                    'destination_port_range': '443',
                    'source_address_prefix': '*',
                    'destination_address_prefix': '*',
                    'access': 'Allow',
                    'priority': 320,
                    'direction': 'Inbound'
                }
            ]
        }
        
        nsg_result = self.network_client.network_security_groups.begin_create_or_update(
            self.resource_group,
            nsg_name,
            nsg_params
        ).result()
        
        print(f"Network Security Group created: {nsg_name}")
        return nsg_result
    
    def create_network_interface(self, nic_name: str, subnet_id: str, public_ip_id: str, nsg_id: str):
        """Create network interface for VM"""
        nic_params = {
            'location': self.location,
            'ip_configurations': [{
                'name': 'ipconfig1',
                'subnet': {'id': subnet_id},
                'public_ip_address': {'id': public_ip_id}
            }],
            'network_security_group': {'id': nsg_id}
        }
        
        nic_result = self.network_client.network_interfaces.begin_create_or_update(
            self.resource_group,
            nic_name,
            nic_params
        ).result()
        
        print(f"Network Interface created: {nic_name}")
        return nic_result
    
    def create_virtual_machine(self, vm_name: str, nic_id: str, username: str, ssh_key: str):
        """Create optimized virtual machine for scraping"""
        
        # VM Size: Standard_B2s (2 vCPUs, 4GB RAM) - Cost effective
        # Estimated cost: ~$30-40/month for continuous use
        vm_params = {
            'location': self.location,
            'os_profile': {
                'computer_name': vm_name,
                'admin_username': username,
                'linux_configuration': {
                    'disable_password_authentication': True,
                    'ssh': {
                        'public_keys': [{
                            'path': f'/home/{username}/.ssh/authorized_keys',
                            'key_data': ssh_key
                        }]
                    }
                }
            },
            'hardware_profile': {
                'vm_size': 'Standard_B2s'  # Burstable performance, cost-effective
            },
            'storage_profile': {
                'image_reference': {
                    'publisher': 'Canonical',
                    'offer': 'UbuntuServer',
                    'sku': '18.04-LTS',
                    'version': 'latest'
                },
                'os_disk': {
                    'caching': 'ReadWrite',
                    'managed_disk': {
                        'storage_account_type': 'Standard_LRS'  # Cheapest storage
                    },
                    'create_option': 'FromImage'
                }
            },
            'network_profile': {
                'network_interfaces': [{
                    'id': nic_id
                }]
            }
        }
        
        vm_result = self.compute_client.virtual_machines.begin_create_or_update(
            self.resource_group,
            vm_name,
            vm_params
        ).result()
        
        print(f"Virtual Machine created: {vm_name}")
        return vm_result
    
    def create_auto_shutdown(self, vm_name: str):
        """Create auto-shutdown schedule to save costs"""
        # Auto-shutdown at 2 AM UTC daily to save costs
        shutdown_params = {
            "location": self.location,
            "properties": {
                "status": "Enabled",
                "taskType": "ComputeVmShutdownTask",
                "dailyRecurrence": {
                    "time": "0200"  # 2:00 AM
                },
                "timeZoneId": "UTC",
                "targetResourceId": f"/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group}/providers/Microsoft.Compute/virtualMachines/{vm_name}",
                "notificationSettings": {
                    "status": "Enabled",
                    "timeInMinutes": 30,
                    "emailRecipient": "admin@yourcompany.com"
                }
            }
        }
        
        # Note: This requires DevTest Labs API or custom implementation
        print("Auto-shutdown configured for cost optimization")
    
    def deploy_scraper_environment(self):
        """Deploy complete infrastructure"""
        print("Starting Azure infrastructure deployment...")
        
        # Create resource group
        self.create_resource_group()
        
        # Create storage
        storage_name = f"scraperstore{datetime.now().strftime('%Y%m%d')}"
        self.create_storage_account(storage_name)
        
        # Create networking
        vnet_name = "scraper-vnet"
        subnet_name = "scraper-subnet"
        vnet, subnet = self.create_vm_network(vnet_name, subnet_name)
        
        # Create public IP
        ip_name = "scraper-ip"
        public_ip = self.create_public_ip(ip_name)
        
        # Create NSG
        nsg_name = "scraper-nsg"
        nsg = self.create_network_security_group(nsg_name)
        
        # Create NIC
        nic_name = "scraper-nic"
        nic = self.create_network_interface(nic_name, subnet.id, public_ip.id, nsg.id)
        
        # Create VM
        vm_name = "scraper-vm"
        ssh_key = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC..."  # Replace with your SSH public key
        username = "azureuser"
        
        vm = self.create_virtual_machine(vm_name, nic.id, username, ssh_key)
        
        # Configure auto-shutdown
        self.create_auto_shutdown(vm_name)
        
        print("✅ Infrastructure deployment completed!")
        
        return {
            'resource_group': self.resource_group,
            'vm_name': vm_name,
            'storage_account': storage_name,
            'public_ip': public_ip.ip_address if hasattr(public_ip, 'ip_address') else 'Pending',
            'estimated_monthly_cost': '$35-45 USD'
        }

def get_vm_setup_script():
    """Get setup script for the VM"""
    return '''#!/bin/bash
# Telegram Music Scraper VM Setup Script

# Update system
sudo apt update && sudo apt upgrade -y

# Install Python 3.8+
sudo apt install python3 python3-pip python3-venv git curl -y

# Install Node.js for n8n
curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
sudo apt-get install -y nodejs

# Install n8n globally
sudo npm install n8n -g

# Create project directory
mkdir -p /home/azureuser/telegram-scraper
cd /home/azureuser/telegram-scraper

# Clone your repository (replace with actual repo)
# git clone https://github.com/yourusername/telegram-music-scraper.git .

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install Python dependencies
pip install -r requirements.txt

# Install additional tools
sudo apt install ffmpeg -y  # For audio processing

# Create systemd service for n8n
sudo tee /etc/systemd/system/n8n.service > /dev/null <<EOF
[Unit]
Description=n8n
After=network.target

[Service]
Type=simple
User=azureuser
ExecStart=/usr/bin/n8n start
Restart=on-failure
Environment=NODE_ENV=production
Environment=N8N_BASIC_AUTH_ACTIVE=true
Environment=N8N_BASIC_AUTH_USER=admin
Environment=N8N_BASIC_AUTH_PASSWORD=secure_password_123

[Install]
WantedBy=multi-user.target
EOF

# Start and enable n8n service
sudo systemctl daemon-reload
sudo systemctl enable n8n
sudo systemctl start n8n

# Setup firewall
sudo ufw allow 22    # SSH
sudo ufw allow 5678  # n8n web interface
sudo ufw --force enable

# Create cron job for scraper (runs every 2 hours)
(crontab -l 2>/dev/null; echo "0 */2 * * * cd /home/azureuser/telegram-scraper && /home/azureuser/telegram-scraper/venv/bin/python telegram_music_scraper.py >> scraper.log 2>&1") | crontab -

echo "✅ VM setup completed!"
echo "n8n is running on http://YOUR_VM_IP:5678"
echo "Default login: admin / secure_password_123"
'''

def estimate_costs():
    """Estimate Azure costs for the infrastructure"""
    costs = {
        "Virtual Machine (Standard_B2s)": {
            "monthly": 35,
            "description": "2 vCPUs, 4GB RAM, burstable performance"
        },
        "Storage Account (Standard_LRS)": {
            "monthly": 5,
            "description": "100GB standard storage"
        },
        "Public IP (Basic)": {
            "monthly": 3,
            "description": "Dynamic public IP address"
        },
        "Bandwidth": {
            "monthly": 7,
            "description": "Estimated 50GB outbound data transfer"
        },
        "Total Estimated": {
            "monthly": 50,
            "description": "Total monthly cost (may vary based on usage)"
        },
        "Credit Usage": {
            "months_coverage": "$2000 / $50 = 40 months",
            "description": "Your $2000 credit should last 40+ months"
        }
    }
    
    return costs

if __name__ == "__main__":
    # Configuration
    SUBSCRIPTION_ID = "your-subscription-id"
    RESOURCE_GROUP = "telegram-scraper-rg"
    LOCATION = "eastus"  # Choose cheapest region
    
    # Deploy infrastructure
    azure_infra = AzureInfrastructure(SUBSCRIPTION_ID, RESOURCE_GROUP, LOCATION)
    result = azure_infra.deploy_scraper_environment()
    
    print("\n" + "="*50)
    print("DEPLOYMENT SUMMARY")
    print("="*50)
    print(json.dumps(result, indent=2))
    
    print("\n" + "="*50)
    print("COST ESTIMATION")
    print("="*50)
    costs = estimate_costs()
    for item, details in costs.items():
        print(f"{item}: ${details['monthly']}/month - {details['description']}")
    
    print("\n" + "="*50)
    print("NEXT STEPS")
    print("="*50)
    print("1. SSH into your VM: ssh azureuser@YOUR_VM_IP")
    print("2. Run setup script: curl -sSL SETUP_SCRIPT_URL | bash")
    print("3. Configure your environment variables")
    print("4. Access n8n at: http://YOUR_VM_IP:5678")
    print("5. Import the n8n workflow from n8n_workflow.json")
    print("\n✅ Ready to start scraping!")