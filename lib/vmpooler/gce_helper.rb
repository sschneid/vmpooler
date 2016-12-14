require 'rubygems' unless defined?(Gem)

module Vmpooler
  class GCEHelper
    def initialize(config, logger, redis, metrics)
      $config  = config
      $logger  = logger
      $redis   = redis
      $metrics = metrics

      ENV['GOOGLE_CLIENT_ID'] = $config[:gce]['client_id']
      ENV['GOOGLE_CLIENT_EMAIL'] = $config[:gce]['client_email']
      ENV['GOOGLE_PRIVATE_KEY'] = $config[:gce]['private_key']
      ENV['GOOGLE_ACCOUNT_TYPE'] = 'service_account'

      @connection = Google::Apis::ComputeV1::ComputeService.new
      @authorization = Google::Auth.get_application_default(
        [
          'https://www.googleapis.com/auth/cloud-platform',
          'https://www.googleapis.com/auth/compute',
        ]
      ) 
      @connection.authorization = @authorization
    end

    def clone_vm(pool)
      # Generate a randomized vm_name
      o = [('a'..'z'), ('0'..'9')].map(&:to_a).flatten
      vm_name = $config[:config]['prefix'] + o[rand(25)] + (0...14).map { o[rand(o.length)] }.join

      begin
        start = Time.now

        # Add VM to Redis inventory ('pending' pool)
        $redis.sadd('vmpooler__pending__' + pool['name'], vm_name)
        $redis.hset('vmpooler__vm__' + vm_name, 'clone', Time.now)
        $redis.hset('vmpooler__vm__' + vm_name, 'template', pool['name'])

        $logger.log('d', "[ ] [#{pool['name']}] '#{vm_name}' is being cloned from '#{pool['template']}'")

        access_config = Google::Apis::ComputeV1::AccessConfig.new
        access_config.name = 'External NAT'
        access_config.type = 'ONE_TO_ONE_NAT'

        network_interface = Google::Apis::ComputeV1::NetworkInterface.new
        network_interface.network = "projects/#{$config[:gce]['project']}/global/networks/default"
        network_interface.access_configs = [ access_config ]

        disk_params = Google::Apis::ComputeV1::AttachedDiskInitializeParams.new
        disk_params.disk_name = vm_name
        disk_params.disk_size_gb = 10
        disk_params.source_image = pool['template']
        disk = Google::Apis::ComputeV1::AttachedDisk.new
        disk.boot = true
        disk.initialize_params = disk_params

        instance = Google::Apis::ComputeV1::Instance.new
        instance.name = vm_name
        instance.machine_type = "zones/#{$config[:gce]['region']}/machineTypes/#{pool['machine_type']}"
        instance.network_interfaces = [ network_interface ]
        instance.disks = [ disk ]

        operation = @connection.insert_instance(
          $config[:gce]['project'],
          $config[:gce]['region'],
          instance
        )
        operation_name = operation.name

        loop do
          operation = @connection.get_zone_operation(
            $config[:gce]['project'],
            $config[:gce]['region'],
            operation_name
          )

          break if operation.status == 'DONE'
          sleep 1
        end

        instance = @connection.get_instance(
          $config[:gce]['project'],
          $config[:gce]['region'],
          vm_name
        )

        $redis.hset('vmpooler__vm__' + vm_name, 'hostname', instance.network_interfaces.first.access_configs.first.nat_ip)
        $redis.hset('vmpooler__vm__' + vm_name, 'ip_address', instance.network_interfaces.first.access_configs.first.nat_ip)

        finish = '%.2f' % (Time.now - start)

        $redis.hset('vmpooler__clone__' + Date.today.to_s, pool['name'] + ':' + vm_name, finish)
        $redis.hset('vmpooler__vm__' + vm_name, 'clone_time', finish)

        $logger.log('s', "[+] [#{pool['name']}] '#{vm_name}' cloned from '#{pool['template']}' in #{finish} seconds")
      rescue
        $redis.smove('vmpooler__pending__' + pool['name'], 'vmpooler__completed__' + pool['name'], vm_name)
        $logger.log('s', "[!] [#{pool['name']}] '#{vm_name}' clone appears to have failed")
      end
    end


    def destroy_vm(vm, pool)
      $logger.log('d', "[ ] [#{pool}] '#{vm}' is being shut down")

      operation = @connection.delete_instance(
        $config[:gce]['project'],
        $config[:gce]['region'],
        vm
      )
      operation_name = operation.name

      loop do
        operation = @connection.get_zone_operation(
          $config[:gce]['project'],
          $config[:gce]['region'],
          operation_name
        )

        break if operation.status == 'DONE'
        sleep 1
      end

      operation = @connection.delete_disk(
        $config[:gce]['project'],
        $config[:gce]['region'],
        vm
      )
      operation_name = operation.name

      loop do
        operation = @connection.get_zone_operation(
          $config[:gce]['project'],
          $config[:gce]['region'],
          operation_name
        )

        break if operation.status == 'DONE'
        sleep 1
      end
    end


    def find_vm(vm)
      # placeholder
      return true
    end


    def get_inventory(pool)
      inventory = {}

      @connection.list_instances(
        $config[:gce]['project'],
        $config[:gce]['region']
      ).items.each do |instance|
        if instance.name.start_with?($config[:config]['prefix'])
          if instance.status == 'RUNNING'
            inventory[instance.name] = 1
          end
        end
      end

      inventory
    end

  end
end
