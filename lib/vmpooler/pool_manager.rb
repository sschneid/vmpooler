module Vmpooler
  class PoolManager
    def initialize(config, logger, redis, metrics)
      $config = config

      # Load logger library
      $logger = logger

      # metrics logging handle
      $metrics = metrics

      # Connect to Redis
      $redis = redis

      # provider object
      $provider = {}

      # Our thread-tracker object
      $threads = {}
    end

    # Check the state of a VM
    def check_pending_vm(vm, pool, timeout)
      Thread.new do
        _check_pending_vm(vm, pool, timeout)
      end
    end

    def _check_pending_vm(vm, pool, timeout)
      host = $provider[pool].find_vm(vm)

      if host
        begin
        hostname = $redis.hget('vmpooler__vm__' + vm, 'hostname') || vm
          Timeout.timeout(5) do
            TCPSocket.new hostname, 22
          end
          move_pending_vm_to_ready(vm, pool, host)
        rescue
          fail_pending_vm(vm, pool, timeout)
        end
      else
        fail_pending_vm(vm, pool, timeout)
      end
    end

    def fail_pending_vm(vm, pool, timeout)
      clone_stamp = $redis.hget('vmpooler__vm__' + vm, 'clone')

      if (clone_stamp) &&
          (((Time.now - Time.parse(clone_stamp)) / 60) > timeout)

        $redis.smove('vmpooler__pending__' + pool, 'vmpooler__completed__' + pool, vm)

        $logger.log('d', "[!] [#{pool}] '#{vm}' marked as 'failed' after #{timeout} minutes")
      end
    end

    def move_pending_vm_to_ready(vm, pool, host)
      case pool['provider']
        when 'vsphere'
          if (host.summary) &&
              (host.summary.guest) &&
              (host.summary.guest.hostName) &&
              (host.summary.guest.hostName == vm)

            begin
              Socket.getaddrinfo(vm, nil)  # WTF?
            rescue
            end
          else
            return
        end
      end

      clone_time = $redis.hget('vmpooler__vm__' + vm, 'clone')
      finish = '%.2f' % (Time.now - Time.parse(clone_time)) if clone_time

      $redis.smove('vmpooler__pending__' + pool, 'vmpooler__ready__' + pool, vm)
      $redis.hset('vmpooler__boot__' + Date.today.to_s, pool + ':' + vm, finish)

      $logger.log('s', "[>] [#{pool}] '#{vm}' moved to 'ready' queue")
    end

    def check_ready_vm(vm, pool, ttl)
      Thread.new do
        if ttl > 0
          if (((Time.now - host.runtime.bootTime) / 60).to_s[/^\d+\.\d{1}/].to_f) > ttl
            $redis.smove('vmpooler__ready__' + pool, 'vmpooler__completed__' + pool, vm)

            $logger.log('d', "[!] [#{pool}] '#{vm}' reached end of TTL after #{ttl} minutes, removed from 'ready' queue")
          end
        end

        check_stamp = $redis.hget('vmpooler__vm__' + vm, 'check')

        if
          (!check_stamp) ||
          (((Time.now - Time.parse(check_stamp)) / 60) > $config[:config]['vm_checktime'])

          $redis.hset('vmpooler__vm__' + vm, 'check', Time.now)

          host = $provider[pool].find_vm(vm) ||
                 $provider[pool].find_vm_heavy(vm)[vm]

          if host
            if
              (host.runtime) &&
              (host.runtime.powerState) &&
              (host.runtime.powerState != 'poweredOn')

              $redis.smove('vmpooler__ready__' + pool, 'vmpooler__completed__' + pool, vm)

              $logger.log('d', "[!] [#{pool}] '#{vm}' appears to be powered off, removed from 'ready' queue")
            end

            if
              (host.summary.guest) &&
              (host.summary.guest.hostName) &&
              (host.summary.guest.hostName != vm)

              $redis.smove('vmpooler__ready__' + pool, 'vmpooler__completed__' + pool, vm)

              $logger.log('d', "[!] [#{pool}] '#{vm}' has mismatched hostname, removed from 'ready' queue")
            end
          else
            $redis.srem('vmpooler__ready__' + pool, vm)

            $logger.log('s', "[!] [#{pool}] '#{vm}' not found in vCenter inventory, removed from 'ready' queue")
          end

          begin
            Timeout.timeout(5) do
              TCPSocket.new vm, 22
            end
          rescue
            if $redis.smove('vmpooler__ready__' + pool, 'vmpooler__completed__' + pool, vm)
              $logger.log('d', "[!] [#{pool}] '#{vm}' is unreachable, removed from 'ready' queue")
            end
          end
        end
      end
    end

    def check_running_vm(vm, pool, ttl)
      Thread.new do
        _check_running_vm(vm, pool, ttl)
      end
    end

    def _check_running_vm(vm, pool, ttl)
      host = $provider[pool].find_vm(vm)

      if host
        queue_from, queue_to = 'running', 'completed'

        # Check that VM is within defined lifetime
        checkouttime = $redis.hget('vmpooler__active__' + pool, vm)
        if checkouttime
          running = (Time.now - Time.parse(checkouttime)) / 60 / 60

          if (ttl.to_i > 0) &&
              (running.to_i >= ttl.to_i)
            move_vm_queue(pool, vm, queue_from, queue_to, "reached end of TTL after #{ttl} hours")
          end
        end
      end
    end

    def move_vm_queue(pool, vm, queue_from, queue_to, msg)
      $redis.smove("vmpooler__#{queue_from}__#{pool}", "vmpooler__#{queue_to}__#{pool}", vm)
      $logger.log('d', "[!] [#{pool}] '#{vm}' #{msg}")
    end

    # Clone a VM
    def clone_vm(pool)
      Thread.new do
        $provider[pool['name']].clone_vm(pool)

        $redis.decr('vmpooler__tasks__clone')

        $metrics.timing("clone.#{pool['name']}", finish)
      end
    end

    # Destroy a VM
    def destroy_vm(vm, pool)
      Thread.new do
        $redis.srem('vmpooler__completed__' + pool['name'], vm)
        $redis.hdel('vmpooler__active__' + pool['name'], vm)
        $redis.hset('vmpooler__vm__' + vm, 'destroy', Time.now)

        # Auto-expire metadata key
        $redis.expire('vmpooler__vm__' + vm, ($config[:redis]['data_ttl'].to_i * 60 * 60))

        start = Time.now

        $provider[pool['name']].destroy_vm(vm, pool['name'])

        finish = '%.2f' % (Time.now - start)

        $logger.log('s', "[-] [#{pool['name']}] '#{vm}' destroyed in #{finish} seconds")
        $metrics.timing("destroy.#{pool['name']}", finish)
      end
    end

    def create_vm_disk(vm, disk_size)
      Thread.new do
        _create_vm_disk(vm, disk_size)
      end
    end

    def _create_vm_disk(vm, disk_size)
      host = $provider['disk_manager'].find_vm(vm) ||
             $provider['disk_manager'].find_vm_heavy(vm)[vm]

      if (host) && ((! disk_size.nil?) && (! disk_size.empty?) && (disk_size.to_i > 0))
        $logger.log('s', "[ ] [disk_manager] '#{vm}' is attaching a #{disk_size}gb disk")

        start = Time.now

        template = $redis.hget('vmpooler__vm__' + vm, 'template')
        datastore = nil

        $config[:pools].each do |pool|
          if pool['name'] == template
            datastore = pool['datastore']
          end
        end

        if ((! datastore.nil?) && (! datastore.empty?))
          $provider['disk_manager'].add_disk(host, disk_size, datastore)

          rdisks = $redis.hget('vmpooler__vm__' + vm, 'disk')
          disks = rdisks ? rdisks.split(':') : []
          disks.push("+#{disk_size}gb")
          $redis.hset('vmpooler__vm__' + vm, 'disk', disks.join(':'))

          finish = '%.2f' % (Time.now - start)

          $logger.log('s', "[+] [disk_manager] '#{vm}' attached #{disk_size}gb disk in #{finish} seconds")
        else
          $logger.log('s', "[+] [disk_manager] '#{vm}' failed to attach disk")
        end
      end
    end

    def create_vm_snapshot(vm, snapshot_name)
      Thread.new do
        _create_vm_snapshot(vm, snapshot_name)
      end
    end

    def _create_vm_snapshot(vm, snapshot_name)
      host = $provider['snapshot_manager'].find_vm(vm) ||
             $provider['snapshot_manager'].find_vm_heavy(vm)[vm]

      if (host) && ((! snapshot_name.nil?) && (! snapshot_name.empty?))
        $logger.log('s', "[ ] [snapshot_manager] '#{vm}' is being snapshotted")

        start = Time.now

        host.CreateSnapshot_Task(
          name: snapshot_name,
          description: 'vmpooler',
          memory: true,
          quiesce: true
        ).wait_for_completion

        finish = '%.2f' % (Time.now - start)

        $redis.hset('vmpooler__vm__' + vm, 'snapshot:' + snapshot_name, Time.now.to_s)

        $logger.log('s', "[+] [snapshot_manager] '#{vm}' snapshot created in #{finish} seconds")
      end
    end

    def revert_vm_snapshot(vm, snapshot_name)
      Thread.new do
        _revert_vm_snapshot(vm, snapshot_name)
      end
    end

    def _revert_vm_snapshot(vm, snapshot_name)
      host = $provider['snapshot_manager'].find_vm(vm) ||
             $provider['snapshot_manager'].find_vm_heavy(vm)[vm]

      if host
        snapshot = $provider['snapshot_manager'].find_snapshot(host, snapshot_name)

        if snapshot
          $logger.log('s', "[ ] [snapshot_manager] '#{vm}' is being reverted to snapshot '#{snapshot_name}'")

          start = Time.now

          snapshot.RevertToSnapshot_Task.wait_for_completion

          finish = '%.2f' % (Time.now - start)

          $logger.log('s', "[<] [snapshot_manager] '#{vm}' reverted to snapshot in #{finish} seconds")
        end
      end
    end

    def check_disk_queue
      $logger.log('d', "[*] [disk_manager] starting worker thread")

      $provider['disk_manager'] ||= Vmpooler::VsphereHelper.new

      $threads['disk_manager'] = Thread.new do
        loop do
          _check_disk_queue
          sleep(5)
        end
      end
    end

    def _check_disk_queue
      vm = $redis.spop('vmpooler__tasks__disk')

      unless vm.nil?
        begin
          vm_name, disk_size = vm.split(':')
          create_vm_disk(vm_name, disk_size)
        rescue
          $logger.log('s', "[!] [disk_manager] disk creation appears to have failed")
        end
      end
    end

    def check_snapshot_queue
      $logger.log('d', "[*] [snapshot_manager] starting worker thread")

      $provider['snapshot_manager'] ||= Vmpooler::VsphereHelper.new

      $threads['snapshot_manager'] = Thread.new do
        loop do
          _check_snapshot_queue
          sleep(5)
        end
      end
    end

    def _check_snapshot_queue
      vm = $redis.spop('vmpooler__tasks__snapshot')

      unless vm.nil?
        begin
          vm_name, snapshot_name = vm.split(':')
          create_vm_snapshot(vm_name, snapshot_name)
        rescue
          $logger.log('s', "[!] [snapshot_manager] snapshot appears to have failed")
        end
      end

      vm = $redis.spop('vmpooler__tasks__snapshot-revert')

      unless vm.nil?
        begin
          vm_name, snapshot_name = vm.split(':')
          revert_vm_snapshot(vm_name, snapshot_name)
        rescue
          $logger.log('s', "[!] [snapshot_manager] snapshot revert appears to have failed")
        end
      end
    end

    def check_pool(pool)
      $logger.log('d', "[*] [#{pool['name']}] starting worker thread")

      case pool['provider']
        when 'vsphere'
          $provider[pool['name']] ||= Vmpooler::VsphereHelper.new($config, $logger, $redis, $metrics)
      end

      $threads[pool['name']] = Thread.new do
        loop do
          _check_pool(pool)
          sleep(5)
        end
      end
    end

    def _check_pool(pool)
      # INVENTORY
      inventory = {}
      begin
        inventory = $provider[pool['name']].get_inventory(pool)

        inventory.keys.each do |vm|
          if
            (! $redis.sismember('vmpooler__running__' + pool['name'], vm)) &&
            (! $redis.sismember('vmpooler__ready__' + pool['name'], vm)) &&
            (! $redis.sismember('vmpooler__pending__' + pool['name'], vm)) &&
            (! $redis.sismember('vmpooler__completed__' + pool['name'], vm)) &&
            (! $redis.sismember('vmpooler__discovered__' + pool['name'], vm))

            $redis.sadd('vmpooler__discovered__' + pool['name'], vm)

            $logger.log('s', "[?] [#{pool['name']}] '#{vm}' added to 'discovered' queue")
          end
        end
      rescue
      end

      # RUNNING
      $redis.smembers('vmpooler__running__' + pool['name']).each do |vm|
        if inventory[vm]
          begin
            check_running_vm(vm, pool['name'], $redis.hget('vmpooler__vm__' + vm, 'lifetime') || $config[:config]['vm_lifetime'] || 12)
          rescue
          end
        end
      end

      # READY
      $redis.smembers('vmpooler__ready__' + pool['name']).each do |vm|
        if inventory[vm]
          begin
            check_ready_vm(vm, pool['name'], pool['ready_ttl'] || 0)
          rescue
          end
        end
      end

      # PENDING
      $redis.smembers('vmpooler__pending__' + pool['name']).each do |vm|
        if inventory[vm]
          begin
            check_pending_vm(vm, pool['name'], pool['timeout'] || $config[:config]['timeout'] || 15)
          rescue
          end
        end
      end

      # COMPLETED
      $redis.smembers('vmpooler__completed__' + pool['name']).each do |vm|
        if inventory[vm]
          begin
            destroy_vm(vm, pool)
          rescue
            $logger.log('s', "[!] [#{pool['name']}] '#{vm}' destroy appears to have failed")
            $redis.srem('vmpooler__completed__' + pool['name'], vm)
            $redis.hdel('vmpooler__active__' + pool['name'], vm)
            $redis.del('vmpooler__vm__' + vm)
          end
        else
          $logger.log('s', "[!] [#{pool['name']}] '#{vm}' not found in inventory, removed from 'completed' queue")
          $redis.srem('vmpooler__completed__' + pool['name'], vm)
          $redis.hdel('vmpooler__active__' + pool['name'], vm)
          $redis.del('vmpooler__vm__' + vm)
        end
      end

      # DISCOVERED
      $redis.smembers('vmpooler__discovered__' + pool['name']).each do |vm|
        %w(pending ready running completed).each do |queue|
          if $redis.sismember('vmpooler__' + queue + '__' + pool['name'], vm)
            $logger.log('d', "[!] [#{pool['name']}] '#{vm}' found in '#{queue}', removed from 'discovered' queue")
            $redis.srem('vmpooler__discovered__' + pool['name'], vm)
          end
        end

        if $redis.sismember('vmpooler__discovered__' + pool['name'], vm)
          $redis.smove('vmpooler__discovered__' + pool['name'], 'vmpooler__completed__' + pool['name'], vm)
        end
      end

      # REPOPULATE
      ready = $redis.scard('vmpooler__ready__' + pool['name'])
      total = $redis.scard('vmpooler__pending__' + pool['name']) + ready

      $metrics.gauge('ready.' + pool['name'], $redis.scard('vmpooler__ready__' + pool['name']))
      $metrics.gauge('running.' + pool['name'], $redis.scard('vmpooler__running__' + pool['name']))

      if $redis.get('vmpooler__empty__' + pool['name'])
        unless ready == 0
          $redis.del('vmpooler__empty__' + pool['name'])
        end
      else
        if ready == 0
          $redis.set('vmpooler__empty__' + pool['name'], 'true')
          $logger.log('s', "[!] [#{pool['name']}] is empty")
        end
      end

      if total < pool['size']
        (1..(pool['size'] - total)).each do |_i|
          if $redis.get('vmpooler__tasks__clone').to_i < $config[:config]['task_limit'].to_i
            begin
              $redis.incr('vmpooler__tasks__clone')
              clone_vm(pool)
            rescue
              $logger.log('s', "[!] [#{pool['name']}] clone appears to have failed")
              $redis.decr('vmpooler__tasks__clone')
            end
          end
        end
      end
    end

    def execute!
      $logger.log('d', 'starting vmpooler')

      # Clear out the tasks manager, as we don't know about any tasks at this point
      $redis.set('vmpooler__tasks__clone', 0)

      loop do
        if $config[:vsphere]
          if ! $threads['disk_manager']
            check_disk_queue
          elsif ! $threads['disk_manager'].alive?
            $logger.log('d', "[!] [disk_manager] worker thread died, restarting")
            check_disk_queue
          end

          if ! $threads['snapshot_manager']
            check_snapshot_queue
          elsif ! $threads['snapshot_manager'].alive?
            $logger.log('d', "[!] [snapshot_manager] worker thread died, restarting")
            check_snapshot_queue
          end
        end

        $config[:pools].each do |pool|
          if ! $threads[pool['name']]
            check_pool(pool)
          elsif ! $threads[pool['name']].alive?
            $logger.log('d', "[!] [#{pool['name']}] worker thread died, restarting")
            check_pool(pool)
          end
        end

        sleep(1)
      end
    end
  end
end
