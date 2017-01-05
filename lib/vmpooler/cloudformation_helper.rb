require 'rubygems' unless defined?(Gem)

module Vmpooler
  class CloudFormationHelper
    def initialize(config, logger, redis, metrics)
      $config  = config
      $logger  = logger
      $redis   = redis
      $metrics = metrics

      @connection = Aws::CloudFormation::Client.new(
        region: $config[:cloudformation]['region'],
        access_key_id: $config[:cloudformation]['access_key_id'],
        secret_access_key: $config[:cloudformation]['secret_access_key']
      )
    end

    def clone_vm(pool)
      begin
        @connection.describe_account_limits()
      rescue
        initialize
      end

      # Generate a randomized stack_name
      o = [('a'..'z'), ('0'..'9')].map(&:to_a).flatten
      stack_name = $config[:config]['prefix'] + o[rand(25)] + (0...14).map { o[rand(o.length)] }.join

      begin
        start = Time.now

        # Add VM to Redis inventory ('pending' pool)
        $redis.sadd('vmpooler__pending__' + pool['name'], stack_name)
        $redis.hset('vmpooler__vm__' + stack_name, 'clone', Time.now)
        $redis.hset('vmpooler__vm__' + stack_name, 'template', pool['name'])

        $logger.log('d', "[ ] [#{pool['name']}] '#{stack_name}' is being cloned from '#{pool['template']}'")

        request = @connection.create_stack({
          stack_name: stack_name,
          template_url: pool['template'],
          timeout_in_minutes: pool['timeout'].to_i,
          capabilities: ['CAPABILITY_IAM'],
          disable_rollback: true,
          parameters: pool['parameters'],
          tags: [{
            key: 'pool',
            value: pool['name']
          }]
        })

        @connection.wait_until(
          :stack_create_complete,
          stack_name: stack_name
        )

        stack = @connection.describe_stacks({ stack_name: stack_name }).stacks[0]
        stack.outputs.each do |output|
          if output.output_key == 'DnsAddress'
            $redis.hset('vmpooler__vm__' + stack_name, 'hostname', output.output_value)
          end

          $redis.hset('vmpooler__vm__' + stack_name, output.output_key, output.output_value)
        end

        finish = '%.2f' % (Time.now - start)

        $redis.hset('vmpooler__clone__' + Date.today.to_s, pool['name'] + ':' + stack_name, finish)
        $redis.hset('vmpooler__vm__' + stack_name, 'clone_time', finish)

        $logger.log('s', "[+] [#{pool['name']}] '#{stack_name}' cloned from '#{pool['template']}' in #{finish} seconds")

        $redis.smove('vmpooler__pending__' + pool['name'], 'vmpooler__ready__' + pool['name'], stack_name)
        $redis.hset('vmpooler__boot__' + Date.today.to_s, pool['name'] + ':' + stack_name, finish)

        $logger.log('s', "[>] [#{pool['name']}] '#{stack_name}' moved to 'ready' queue")
      rescue
        $redis.smove('vmpooler__pending__' + pool['name'], 'vmpooler__completed__' + pool['name'], stack_name)
        $logger.log('s', "[!] [#{pool['name']}] '#{stack_name}' clone appears to have failed")
      end
    end


    def destroy_vm(vm, pool)
      begin
        @connection.describe_account_limits()
      rescue
        initialize
      end

      $logger.log('d', "[ ] [#{pool}] '#{vm}' is being shut down")

      @connection.delete_stack({
        stack_name: vm
      })

      @connection.wait_until(
        :stack_delete_complete,
        stack_name: vm
      )

      vm
    end


    def find_vm(vm)
      begin
        @connection.describe_account_limits()
      rescue
        initialize
      end

      stack = @connection.describe_stacks({ stack_name: vm }).stacks[0]

      if [
        'CREATE_IN_PROGRESS',
        'CREATE_COMPLETE',
        'CREATE_FAILED',
        'DELETE_FAILED'
      ].include? stack.stacks[0].stack_status
        return true
      else
        return false
      end
    end


    def find_vm_heavy(vm)
      begin
        @connection.describe_account_limits()
      rescue
        initialize
      end

      find_vm(vm)
    end


    def get_inventory(pool)
      begin
        @connection.describe_account_limits()
      rescue
        initialize
      end

      inventory = {}

      stacks = @connection.list_stacks({
        stack_status_filter: [
          'CREATE_IN_PROGRESS',
          'CREATE_COMPLETE',
          'CREATE_FAILED',
          'DELETE_FAILED'
        ]
      })

      stacks.stack_summaries.each do |stack|
        if stack.stack_name.start_with?($config[:config]['prefix'])
          @connection.describe_stacks({ stack_name: stack.stack_name }).stacks[0].tags.each do |tag|
            if (tag.key == 'pool') and (tag.value == pool['name'])
              inventory[stack.stack_name] = 1
            end
          end
        end
      end

      inventory
    end

  end
end
