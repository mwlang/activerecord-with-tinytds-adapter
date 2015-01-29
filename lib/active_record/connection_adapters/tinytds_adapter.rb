require 'active_record/connection_adapters/abstract_adapter'

require 'bigdecimal'
require 'bigdecimal/util'

module ActiveRecord
  class Base
    def self.tinytds_connection(config) #:nodoc:
      require_library_or_gem 'tiny_tds' unless self.class.const_defined?(:TinyTds)
      
      config = config.symbolize_keys

      options = {}

      if database = config[:database]
        options[:database] = database
      end

      if config[:dataserver]
        options[:dataserver] = config[:dataserver].to_s
      else
        options[:host] = config[:host] ? config[:host].to_s : 'localhost'
        if config[:port]
          options[:port] = config[:port].to_i
        end
      end

      if config[:timeout]
        options[:timeout] = config[:timeout].to_i
      end
      
      options[:username] = config[:username] ? config[:username].to_s : 'sa'
      options[:password] = config[:password] ? config[:password].to_s : ''
      log_ddl = ['true','yes','1'].include? config[:log_ddl].to_s
      
      connection = TinyTds::Client.new options
      ConnectionAdapters::TinyTdsAdapter.new(connection, logger, log_ddl, options)
    end
  end # class Base

  module ConnectionAdapters
    class TinyTdsColumn < Column# :nodoc:
      attr_reader :identity, :is_special

      def initialize(name, default, sql_type, null, identity) # TODO: check ok to remove scale_value = 0
        super(name, default_value(default), sql_type, null)
        @identity = identity
        @is_special = sql_type =~ /text|ntext|image/i

        # SQL Server only supports limits on *char and float types
        @limit = nil unless @type == :float or @type == :string
      end

      def default_value(value)
        case value
        when nil, '(null)', '(NULL)'
          nil
        else
          match_data = value.match(/\A\(+N?'?(.*?)'?\)+\Z/m)
          match_data ? match_data[1] : nil
        end
      end
      
      def is_identity?
        @identity
      end
      
      def simplified_type(field_type)
        case field_type
          when /money/i             then :decimal
          when /image/i             then :binary
          when /bit/i               then :boolean
          when /uniqueidentifier/i  then :string
          else super
        end
      end

      def type_cast(value)
        return nil if value.nil? || value == '(NULL)'
        case type
        when :datetime  then cast_to_datetime(value)
        when :timestamp then cast_to_time(value)
        when :time      then cast_to_time(value)
        when :date      then cast_to_datetime(value)
        when :boolean   then value == true or (value =~ /^t(rue)?$/i) == 0 or value.to_s == '1'
        else super
        end
      end
      
      def cast_to_time(value)
        return value if value.is_a?(Time)
        time_array = ParseDate.parsedate(value)
        Time.send(Base.default_timezone, *time_array) rescue nil
      end

      def cast_to_datetime(value)
        if value.is_a?(Time)
          if value.year != 0 and value.month != 0 and value.day != 0
            return value
          else
            return Time.mktime(2000, 1, 1, value.hour, value.min, value.sec) rescue nil
          end
        end
   
        if value.is_a?(DateTime)
          return Time.mktime(value.year, value.mon, value.day, value.hour, value.min, value.sec)
        end
        
        return cast_to_time(value) if value.is_a?(Date) or value.is_a?(String) rescue nil
        value
      end
      
      # TODO: Find less hack way to convert DateTime objects into Times
      
      def self.string_to_time(value)
        if value.is_a?(DateTime)
          return Time.mktime(value.year, value.mon, value.day, value.hour, value.min, value.sec)
        else
          super
        end
      end

      # These methods will only allow the adapter to insert binary data with a length of 7K or less
      # because of a SQL Server statement length policy.
      def self.string_to_binary(value)
        value.gsub(/(\r|\n|\0|\x1a)/) do
          case $1
            when "\r"   then  "%00"
            when "\n"   then  "%01"
            when "\0"   then  "%02"
            when "\x1a" then  "%03"
          end
        end
      end

      def self.binary_to_string(value)
        value.gsub(/(%00|%01|%02|%03)/) do
          case $1
            when "%00"    then  "\r"
            when "%01"    then  "\n"
            when "%02\0"  then  "\0"
            when "%03"    then  "\x1a"
          end
        end
      end
    end

    # Options:
    
    # :username - The database server user.
    # :password - The user password.
    # :dataserver - Can be the name for your data server as defined in freetds.conf. Raw hostname or hostname:port will work here too. FreeTDS says that named instance like 'localhost\SQLEXPRESS' work too, but I highly suggest that you use the :host and :port options below. Google how to find your host port if you are using named instances or go here.
    # :host - Used if :dataserver blank. Can be an host name or IP.
    # :port - Defaults to 1433. Only used if :host is used.
    # :database - The default database to use.
    class TinyTdsAdapter < AbstractAdapter

      LOST_CONNECTION_EXCEPTIONS  = ['TinyTds::Error']

      LOST_CONNECTION_MESSAGES = [
        /link failure/, 
        /server failed/, 
        /connection was already closed/, 
        /invalid handle/i,
        /current state is closed/, 
        /network-related/
      ]
    
      def initialize(connection, logger, log_ddl, connection_options=nil)
        super(connection, logger)
        @log_ddl = log_ddl
        @connection_options = connection_options
      end

      def raw_connection_do(sql)
        raw_connection.execute(sql).do
      end

      def native_database_types
        {
          :primary_key => "int NOT NULL IDENTITY(1, 1) PRIMARY KEY",
          :string      => { :name => "varchar", :limit => 255  },
          :text        => { :name => "text" },
          :integer     => { :name => "int" },
          :float       => { :name => "float", :limit => 8 },
          :decimal     => { :name => "decimal" },
          :datetime    => { :name => "datetime" },
          :timestamp   => { :name => "datetime" },
          :time        => { :name => "datetime" },
          :date        => { :name => "datetime" },
          :binary      => { :name => "image"},
          :boolean     => { :name => "bit"}
        }
      end

      def adapter_name
        'TinyTds'
      end
      
      def supports_migrations? #:nodoc:
        true
      end

      def type_to_sql(type, limit = nil, precision = nil, scale = nil) #:nodoc:
        return super unless type.to_s == 'integer'

        if limit.nil? || limit == 4
          'integer'
        elsif limit < 4
          'smallint'
        else
          'bigint'
        end
      end

      # CONNECTION MANAGEMENT ====================================#

      # Returns true if the connection is active.
      def active?
        raw_connection.active?
      end

      # Reconnects to the database, returns false if no connection could be made.
      def reconnect!
        disconnect!
        @connection = TinyTds::Client.new @connection_options
      rescue TinyTds::Error => e
        @logger.warn "#{adapter_name} reconnection failed: #{e.message}" if @logger
        false
      end
      
      # Disconnects from the database
      
      def disconnect!
        raw_connection.close
      end

      def columns(table_name, name = nil)
        return [] if table_name.blank?
        table_name = table_name.to_s if table_name.is_a?(Symbol)
        table_name = table_name.split('.')[-1] unless table_name.nil?
        table_name = table_name.gsub(/[\[\]]/, '')

        sql = %Q{
          SELECT 
            cols.COLUMN_NAME as name,  
            cols.COLUMN_DEFAULT as default_value,
            cols.NUMERIC_SCALE as numeric_scale,
            cols.NUMERIC_PRECISION as numeric_precision, 
            cols.DATA_TYPE as data_type, 
            cols.IS_NULLABLE As is_nullable,  
            COL_LENGTH(cols.TABLE_NAME, cols.COLUMN_NAME) as length,  
            COLUMNPROPERTY(OBJECT_ID(cols.TABLE_NAME), cols.COLUMN_NAME, 'IsIdentity') as is_identity,  
            cols.NUMERIC_SCALE as scale 
          FROM INFORMATION_SCHEMA.COLUMNS cols 
          WHERE cols.TABLE_NAME = '#{table_name}'   
        }

        result = @log_ddl ? log(sql, name) { raw_connection.execute(sql) } : raw_connection.execute(sql)

        columns = []
        info = result.each
        result.each do |values|
          default = values["default_value"]
          if values["data_type"] =~ /numeric|decimal/i
            data_type = "#{values['data_type']}(#{values['numeric_precision']},#{values['numeric_scale']})"
          else
            data_type = "#{values['data_type']}(#{values['length']})"
          end
          is_identity = values['is_identity'] == 1
          is_nullable = values['is_nullable'] == 'YES'
          columns << TinyTdsColumn.new(values['name'], default, data_type, is_nullable, is_identity)
        end
        result.do
        columns
      end

      def select_one(sql, name = nil)
        result = log(sql, name) { raw_connection.execute(sql) }
        result.each(:first => true)[0] rescue nil
      end
      
      def select_value(sql, name="SELECT VALUE")
        select_one(sql, name).values[0] rescue nil
      end
      
      def select_values(sql)
        result = log(sql, 'SELECT_VALUES') { raw_connection.execute(sql) }
        result.map{ |row| row.values[0] }
      end
      
      def insert(sql, name = nil, pk = nil, id_value = nil, sequence_name = nil)
        log(sql, name || 'INSERT') do
          if table_name = query_requires_identity_insert?(sql)
            with_identity_insert_enabled(table_name) { raw_connection.execute(sql).insert || id_value }
          else
            raw_connection.execute(sql).insert || id_value 
          end
        end
      end

      def update(sql, name = nil)
        execute(sql.chars.to_s, name)
      end
      
      alias_method :delete, :update

      def query_requires_identity_insert?(sql)
        if insert_sql?(sql)
          table_name = get_table_name(sql)
          id_column = identity_column(table_name)
          id_column && sql =~ /^\s*INSERT[^(]+\([^)]*\b(#{id_column.name})\b,?[^)]*\)/i ? quote_table_name(table_name) : false
        else
          false
        end
      end
      
      def identity_column(table_name)
        columns(table_name).detect(&:is_identity?)
      end

      def execute(sql, name = nil, skip_logging = false)
        begin
          return insert(sql) if insert_sql?(sql)
          log(sql, name || 'EXECUTE') do
            with_auto_reconnect do  
              result = raw_connection.execute sql
              result.each { |row| yield(row) if block_given? }
              result.do
            end
          end
            
        rescue TinyTds::Error => e
          raise ActiveRecord::StatementInvalid.new(e.message)
        end
      end
      
      def lost_connection_exceptions
        @lost_connection_exceptions ||= LOST_CONNECTION_EXCEPTIONS.map(&:constantize)
      end
      
      def lost_connection_messages
        LOST_CONNECTION_MESSAGES
      end

      # from activerecord-sqlserver-adapter
      def with_auto_reconnect
        begin
          yield
        rescue *lost_connection_exceptions => e
          if lost_connection_messages.any? { |lcm| e.message =~ lcm }
            retry if auto_reconnected?
          end
          raise
        end
      end
      
      # from activerecord-sqlserver-adapter
      def do_execute(sql,name=nil)
        log(sql, name || 'EXECUTE') do
          with_auto_reconnect { raw_connection_do(sql) }
        end
      end
      
      def begin_db_transaction
        do_execute "BEGIN TRANSACTION"
      end

      def commit_db_transaction
        do_execute "COMMIT TRANSACTION"
      end

      def rollback_db_transaction
        do_execute "ROLLBACK TRANSACTION" rescue nil
      end
      
      def quote_utf8_string(value, as_national)
        prefix = as_national ? '' : ''
        ::Iconv.conv('UTF-8//IGNORE', 'UTF-8', "#{prefix}'#{quote_string(value)}'")
      end
      
      def quote(value, column = nil)
        return value.quoted_id if value.respond_to?(:quoted_id)
        return 'NULL' if value.to_s == 'NULL'

        case value
          when String, ActiveSupport::Multibyte::Chars
            value = value.to_s
            if column && column.type == :binary && column.class.respond_to?(:string_to_binary)
              quote_utf8_string(column.class.string_to_binary(value), false)
            elsif column && [:integer, :float].include?(column.type)
              value = column.type == :integer ? value.to_i : value.to_f
              value.to_s
            else
              quote_utf8_string(value, true)
            end
          when TrueClass             then '1'
          when FalseClass            then '0'
          when Time, DateTime        then "'#{value.strftime("%Y%m%d %H:%M:%S")}'"
          when Date                  then "'#{value.strftime("%Y%m%d")}'"
          else                       super
        end
      end

      def quote_string(string)
        string.gsub(/\'/, "''")
      end

      def add_limit_offset!(sql, options)
        if options[:limit] and options[:offset]
          total_rows = select_value("SELECT count(*) as TotalRows from (#{sql.gsub(/\bSELECT(\s+DISTINCT)?\b/i, "SELECT#{$1} TOP 1000000000")}) tally").to_i
          if (options[:limit] + options[:offset]) >= total_rows
            options[:limit] = (total_rows - options[:offset] >= 0) ? (total_rows - options[:offset]) : 0
          end
          sql.sub!(/^\s*SELECT(\s+DISTINCT)?/i, "SELECT * FROM (SELECT TOP #{options[:limit]} * FROM (SELECT#{$1} TOP #{options[:limit] + options[:offset]} ")
          sql << ") AS tmp1"
          if options[:order]
            options[:order] = options[:order].split(',').map do |field|
              parts = field.split(" ")
              tc = parts[0]
              if sql =~ /\.\[/ and tc =~ /\./ # if column quoting used in query
                tc.gsub!(/\./, '\\.\\[')
                tc << '\\]'
              end
              if sql =~ /#{tc} AS (t\d_r\d\d?)/
                parts[0] = $1
              elsif parts[0] =~ /\w+\.(\w+)/
                parts[0] = $1
              end
              parts.join(' ')
            end.join(', ')
            sql << " ORDER BY #{change_order_direction(options[:order])}) AS tmp2 ORDER BY 1"
          else
            sql << " ) AS tmp2"
          end
        elsif sql !~ /^\s*SELECT (@@|COUNT\()/i
          sql.sub!(/^\s*SELECT(\s+DISTINCT)?/i) do
            "SELECT#{$1} TOP #{options[:limit]}"
          end unless options[:limit].nil?
        end
      end

      def recreate_database(name)
        drop_database(name)
        create_database(name)
      end

      def drop_database(name)
        execute "DROP DATABASE #{name}"
      end

      def create_database(name)
        execute "CREATE DATABASE #{name}"
      end
   
      def current_database
        select_value('select DB_NAME()')
      end

      def tables(name = nil)
        select_values "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME <> 'dtproperties'"
      end


      def indexes(table_name, name = nil)
        unquoted_table_name = unqualify_table_name(table_name)
        select("EXEC sp_helpindex #{quote_table_name(unquoted_table_name)}",name).inject([]) do |indexes,index|
          if index['index_description'] =~ /primary key/
            indexes
          else
            name    = index['index_name']
            unique  = index['index_description'] =~ /unique/
            columns = index['index_keys'].split(',').map do |column|
              column.strip!
              column.gsub! '(-)', '' if column.ends_with?('(-)')
              column
            end
            indexes << IndexDefinition.new(table_name, name, unique, columns)
          end
        end
      end
            
      def rename_table(name, new_name)
        execute "EXEC sp_rename '#{name}', '#{new_name}'"
      end
      
      # Adds a new column to the named table.
      # See TableDefinition#column for details of the options you can use.
      def add_column(table_name, column_name, type, options = {})
        add_column_sql = "ALTER TABLE #{table_name} ADD #{quote_column_name(column_name)} #{type_to_sql(type, options[:limit], options[:precision], options[:scale])}"
        add_column_options!(add_column_sql, options)
        # TODO: Add support to mimic date columns, using constraints to mark them as such in the database
        # add_column_sql << " CONSTRAINT ck__#{table_name}__#{column_name}__date_only CHECK ( CONVERT(CHAR(12), #{quote_column_name(column_name)}, 14)='00:00:00:000' )" if type == :date       
        execute(add_column_sql)
      end
       
      def rename_column(table, column, new_column_name)
        execute "EXEC sp_rename '#{table}.#{column}', '#{new_column_name}'"
      end
      
      def change_column(table_name, column_name, type, options = {}) #:nodoc:
        sql_commands = ["ALTER TABLE #{table_name} ALTER COLUMN #{column_name} #{type_to_sql(type, options[:limit], options[:precision], options[:scale])}"]
        if options_include_default?(options)
          remove_default_constraint(table_name, column_name)
          sql_commands << "ALTER TABLE #{table_name} ADD CONSTRAINT DF_#{table_name}_#{column_name} DEFAULT #{quote(options[:default], options[:column])} FOR #{column_name}"
        end
        sql_commands.each {|c|
          execute(c)
        }
      end
      
      def remove_column(table_name, column_name)
        remove_check_constraints(table_name, column_name)
        remove_default_constraint(table_name, column_name)
        execute "ALTER TABLE [#{table_name}] DROP COLUMN [#{column_name}]"
      end
      
      def remove_default_constraint(table_name, column_name)
        constraints = select "select def.name from sysobjects def, syscolumns col, sysobjects tab where col.cdefault = def.id and col.name = '#{column_name}' and tab.name = '#{table_name}' and col.id = tab.id"
        
        constraints.each do |constraint|
          execute "ALTER TABLE #{table_name} DROP CONSTRAINT #{constraint["name"]}"
        end
      end
      
      def remove_check_constraints(table_name, column_name)
        # TODO remove all constraints in single method
        constraints = select "SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE where TABLE_NAME = '#{table_name}' and COLUMN_NAME = '#{column_name}'"
        constraints.each do |constraint|
          execute "ALTER TABLE #{table_name} DROP CONSTRAINT #{constraint["CONSTRAINT_NAME"]}"
        end
      end
      
      def remove_index(table_name, options = {})
        execute "DROP INDEX #{table_name}.#{quote_column_name(index_name(table_name, options))}"
      end


      def quote_string(string)
        string.to_s.gsub(/\'/, "''")
      end

      def quote_column_name(column_name)
        column_name.to_s.split('.').map{ |name| name =~ /^\[.*\]$/ ? name : "[#{name}]" }.join('.')
      end

      def quote_table_name(table_name)
        return table_name if table_name =~ /^\[.*\]$/
        quote_column_name(table_name)
      end

      def quoted_true
        '1'
      end

      def quoted_false
        '0'
      end

      def quoted_date(value)
        if value.acts_like?(:time) && value.respond_to?(:usec)
          "#{super}.#{sprintf("%03d",value.usec/1000)}"
        else
          super
        end
      end

      def unqualify_table_name(table_name)
        table_name.to_s.split('.').last.gsub(/[\[\]]/,'')
      end

      def unqualify_db_name(table_name)
        table_names = table_name.to_s.split('.')
        table_names.length == 3 ? table_names.first.tr('[]','') : nil
      end

      private 
      
        def select(sql, name = nil)
          log(sql, name || 'SELECT') do 
            repair_special_columns(sql)
            result = raw_connection.execute(sql)
            rows = result.map
            result.do
            rows
          end
        end

        # Turns IDENTITY_INSERT ON for table during execution of the block
        # N.B. This sets the state of IDENTITY_INSERT to OFF after the
        # block has been executed without regard to its previous state

        def with_identity_insert_enabled(table_name, &block)
          set_identity_insert(table_name, true)
          yield
        ensure
          set_identity_insert(table_name, false)  
        end
        
        def set_identity_insert(table_name, enable = true)
          execute "SET IDENTITY_INSERT #{table_name} #{enable ? 'ON' : 'OFF'}"
        rescue Exception => e
          raise ActiveRecordError, "IDENTITY_INSERT could not be turned #{enable ? 'ON' : 'OFF'} for table #{table_name}"  
        end

        def get_table_name(sql)
          if sql =~ /^\s*insert\s+into\s+([^\(\s]+)\s*|^\s*update\s+([^\(\s]+)\s*/i
            $1
          elsif sql =~ /from\s+([^\(\s]+)\s*/i
            $1
          else
            nil
          end
        end

        def identity_column(table_name)
          @table_columns ||= {}
          @table_columns[table_name] ||= columns(table_name)
          column = @table_columns[table_name].detect{|col| col.identity}
          column ? column.name : nil
        end

        def insert_sql?(sql)
          !(sql =~ /^\s*INSERT/i).nil?
        end

        def query_requires_identity_insert?(sql)
          table_name = get_table_name(sql)
          id_column = identity_column(table_name)
          sql =~ /\[#{id_column}\]/ ? table_name : nil
        end

        def sql_for_association_limiting?(sql)
          if md = sql.match(/^\s*SELECT(.*)FROM.*GROUP BY.*ORDER BY.*/im)
            select_froms = md[1].split(',')
            select_froms.size == 1 && !select_froms.first.include?('*')
          end
        end

        def change_order_direction(order)
          order.split(",").collect {|fragment|
            case fragment
              when  /\bDESC\b/i     then fragment.gsub(/\bDESC\b/i, "ASC")
              when  /\bASC\b/i      then fragment.gsub(/\bASC\b/i, "DESC")
              else                  String.new(fragment).split(',').join(' DESC,') + ' DESC'
            end
          }.join(",")
        end

        def get_special_columns(table_name)
          special = []
          @table_columns ||= {}
          @table_columns[table_name] ||= columns(table_name)
          @table_columns[table_name].each do |col|
            special << col.name if col.is_special
          end
          special
        end

        def repair_special_columns(sql)
          special_cols = get_special_columns(get_table_name(sql))
          for col in special_cols.to_a
            sql.gsub!(Regexp.new(" #{col.to_s} = "), " #{col.to_s} LIKE ")
            sql.gsub!(/ORDER BY #{col.to_s}/i, '')
          end
          sql
        end

    end #class TinyTdsAdapter < AbstractAdapter
  end #module ConnectionAdapters
end #module ActiveRecord
