require 'rubygems' unless defined?(Gem)

module Vmpooler
  require 'date'
  require 'json'
  require 'open-uri'
  require 'redis'
  require 'sinatra/base'
  require 'time'
  require 'timeout'
  require 'yaml'
  require 'set'

  def self.load_library(library)
    begin
      require "vmpooler/#{library}"
    rescue LoadError
      require File.expand_path(File.join(File.dirname(__FILE__), 'vmpooler', library))
    end
  end

  %w( api graphite logger pool_manager statsd dummy_statsd ).each do |lib|
    load_library(lib)
  end

  def self.config(filepath='vmpooler.yaml')
    # Load the configuration file
    config_file = File.expand_path(filepath)
    parsed_config = YAML.load_file(config_file)

    # Set some defaults
    parsed_config[:redis]             ||= {}
    parsed_config[:redis]['server']   ||= 'localhost'
    parsed_config[:redis]['data_ttl'] ||= 168

    parsed_config[:config]['task_limit']   ||= 10
    parsed_config[:config]['vm_checktime'] ||= 15
    parsed_config[:config]['vm_lifetime']  ||= 24
    parsed_config[:config]['prefix']       ||= ''

    # Load provider libraries and helpers
    if parsed_config[:cloudformation]
      require 'aws-sdk'
      load_library('cloudformation_helper')
    end
    if parsed_config[:gce]
      require 'google/apis/compute_v1'
      load_library('gce_helper')
    end
    if parsed_config[:ec2]
      require 'aws-sdk'
      load_library('ec2_helper')
    end
    if parsed_config[:vsphere]
      require 'rbvmomi'
      load_library('vsphere_helper')
    end

    # Create an index of pool aliases
    parsed_config[:pool_names] = Set.new
    parsed_config[:pools].each do |pool|
      parsed_config[:pool_names] << pool['name']
      if pool['alias']
        if pool['alias'].kind_of?(Array)
          pool['alias'].each do |a|
            parsed_config[:alias] ||= {}
            parsed_config[:alias][a] = pool['name']
            parsed_config[:pool_names] << a
          end
        elsif pool['alias'].kind_of?(String)
          parsed_config[:alias][pool['alias']] = pool['name']
          parsed_config[:pool_names] << pool['alias']
        end
      end
    end

    if parsed_config[:tagfilter]
      parsed_config[:tagfilter].keys.each do |tag|
        parsed_config[:tagfilter][tag] = Regexp.new(parsed_config[:tagfilter][tag])
      end
    end

    parsed_config[:uptime] = Time.now
    parsed_config
  end

  def self.new_redis(host='localhost')
    Redis.new(host: host)
  end

  def self.new_logger(logfile)
    Vmpooler::Logger.new logfile
  end

  def self.new_metrics(params)
    if params[:statsd]
      Vmpooler::Statsd.new(params[:statsd])
    elsif params[:graphite]
      Vmpooler::Graphite.new(params[:graphite])
    else
      Vmpooler::DummyStatsd.new
    end
  end

  def self.pools(conf)
    conf[:pools]
  end
end
