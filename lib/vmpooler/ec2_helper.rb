require 'rubygems' unless defined?(Gem)

module Vmpooler
  class Ec2Helper
    def initialize(config, logger, redis, metrics)
      $config  = config
      $logger  = logger
      $redis   = redis
      $metrics = metrics

      @connection = Aws::EC2::Client.new(
        region: $config[:ec2]['region'],
        access_key_id: $config[:ec2]['access_key_id'],
        secret_access_key: $config[:ec2]['secret_access_key']
      )
      begin
        @connection.describe_key_pairs({
          key_names: ['vmpooler-ec2-keypair']
        })
      rescue
        @connection.create_key_pair({
          key_name: 'vmpooler-ec2-keypair'
        })
      end
    end

    def find_image(image_name)
      begin
        @connection.describe_account_attributes()
      rescue
        initialize
      end

      image = @connection.describe_images({
        dry_run: false,
        image_ids: [image_name]
      })

      return image
    end

    def clone_vm(pool)
      begin
        @connection.describe_account_attributes()
      rescue
        initialize
      end

      begin
        start = Time.now

        # Check that the template (ami) exists
        if ! $provider[pool['name']].find_image(pool['template'])
          $logger.log('s', "[!] [#{pool['name']}] cannot find template '#{pool['template']}'!")
          fail 'Please provide a full path to the template'
        end

        # Get an instance reservation & id
        reservation = @connection.run_instances(
          dry_run: false,
          min_count: 1,
          max_count: 1,
          image_id: pool['template'],
          instance_type: pool['instance_type'],
          key_name: 'vmpooler-ec2-keypair'
        )

        instance_id = reservation.instances[0].instance_id

        @connection.create_tags({
          resources: [instance_id],
          tags: [{ key: 'vmpooler_pool', value: pool['name'] }]
        })

        $logger.log('d', "[ ] [#{pool['name']}] '#{instance_id}' is being cloned from '#{pool['template']}'")

        # Add VM to Redis inventory ('pending' pool)
        $redis.sadd('vmpooler__pending__' + pool['name'], instance_id)
        $redis.hset('vmpooler__vm__' + instance_id, 'clone', Time.now)
        $redis.hset('vmpooler__vm__' + instance_id, 'template', pool['name'])

        @connection.wait_until(
          :instance_running,
          instance_ids: [ instance_id ]
        )

        instance = @connection.describe_instances({ instance_ids: [ instance_id ] }).reservations[0].instances[0]

        $redis.hset('vmpooler__vm__' + instance_id, 'hostname', instance['public_dns_name'])
        $redis.hset('vmpooler__vm__' + instance_id, 'ip_address', instance['public_ip_address'])
        %w( instance_id image_id private_dns_name private_ip_address ).each do |key|
          $redis.hset('vmpooler__vm__' + instance_id, key, instance[key])
        end

        finish = '%.2f' % (Time.now - start)

        $redis.hset('vmpooler__clone__' + Date.today.to_s, pool['name'] + ':' + instance_id, finish)
        $redis.hset('vmpooler__vm__' + instance_id, 'clone_time', finish)

        $logger.log('s', "[+] [#{pool['name']}] '#{instance_id}' cloned from '#{pool['template']}' in #{finish} seconds")
      rescue
        $logger.log('s', "[!] [#{pool['name']}] clone appears to have failed")
      end
    end


    def destroy_vm(vm, pool)
      begin
        @connection.describe_account_attributes()
      rescue
        initialize
      end

      $logger.log('d', "[ ] [#{pool}] '#{vm}' is being shut down")

      @connection.terminate_instances({
        instance_ids: [ vm ]
      })

      @connection.wait_until(
        :instance_terminated,
        instance_ids: [ vm ]
      )
    end


    def find_vm(vm)
      begin
        @connection.describe_account_attributes()
      rescue
        initialize
      end

      # placeholder
      return true
    end


    def get_inventory(pool)
      begin
        @connection.describe_account_attributes()
      rescue
        initialize
      end

      inventory = {}

      instances = @connection.describe_instances(
        filters: [
          { name: 'instance-state-name', values: ['pending', 'running'] },
          { name: 'tag:vmpooler_pool', values: [pool['name']] }
        ]
      )

      instances['reservations'].each do |reservation|
        reservation['instances'].each do |instance|
          inventory[instance.instance_id] = 1
        end
      end

      inventory
    end

  end
end
