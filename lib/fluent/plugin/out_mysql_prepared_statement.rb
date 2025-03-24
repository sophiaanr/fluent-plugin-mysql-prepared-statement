# -*- encoding : utf-8 -*-
module Fluent
  class Fluent::MysqlPreparedStatementOutput < Fluent::BufferedOutput
    Fluent::Plugin.register_output('mysql_prepared_statement', self)

    # Define `router` method of v0.12 to support v0.10.57 or earlier
    unless method_defined?(:router)
      define_method("router") { Fluent::Engine }
    end

    config_param :host, :string, :default => "127.0.0.1"
    config_param :output_tag, :string
    config_param :port, :integer, :default => 3306
    config_param :database, :string
    config_param :username, :string
    config_param :password, :string, :default => '', :secret => true
    
    config_section :sql, param_name: :sql_statements, required: true, multi: true do
      config_param :key_names, :string
      config_param :mysql, :string
    end

    attr_accessor :handler

    def initialize
      super
      require 'mysql2-cs-bind'
    end

    def configure(conf)
      super

      if @sql_statements.empty?
        raise Fluent::ConfigError, "<sql> MUST be specified, but missing"
      end
      
      @sqls = @sql_statements.map do |entry|
        {
          mysql: entry.mysql,
          key_names: entry.key_names.split(',')
        }
      end

      @sqls.each do |entry|
        begin
          Mysql2::Client.pseudo_bind(entry[:mysql], entry[:key_names].map{ nil })
        rescue ArgumentError => e
          raise Fluent::ConfigError, "mismatch between sql placeholders and key_names in query: #{entry[:mysql]}"
        end
      end

      $log.info "Loaded SQL statements: #{@sqls.map { |s| s[:mysql] }}"
    end

    def start
      super
    end

    def shutdown
      super
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def client
      Mysql2::Client.new({
          :host => @host,
          :port => @port,
          :username => @username,
          :password => @password,
          :database => @database,
          :flags => Mysql2::Client::MULTI_STATEMENTS
        })
    end

    def write(chunk)
      @handler = client
      $log.info "Executing MySQL queries..."
      chunk.msgpack_each do |tag, time, data|
        results = get_exec_result(data)
        results.each { |result| router.emit(@output_tag, Fluent::Engine.now, result) }
      end
      @handler.close
    end

    def get_exec_result(data)
      results = []
      @sqls.each do |entry|
        sql = entry[:mysql]
        $log.info "executing #{sql}"
        key_names = entry[:key_names]
        values = key_names.map { |k| data[k] }

        stmt = @handler.xquery(sql, values)
        next if stmt.nil?
        stmt.each { |row| results.push(row) }
      end
      results
    end
  end
end

