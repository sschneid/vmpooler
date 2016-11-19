require 'yaml'
require 'aws-sdk'

config_file = File.expand_path('vmpooler.yaml')
ec2 = YAML.load_file(config_file)[:ec2]

@connection = Aws::EC2::Client.new(
  region: ec2['region'],
  access_key_id: ec2['access_key_id'],
  secret_access_key: ec2['secret_access_key']
)

key_pair = @connection.create_key_pair({ key_name: 'vmpooler-ec2-keypair' })

print key_pair['key_material']
