module Replicate
  def replicate(table, opts)
    action = opts.delete(:action) || :create
    trigger = Trigger.new(table, opts)

    case action
    when :create
      execute(trigger.create_sql)
    when :drop
      execute(trigger.drop_sql)
    when :initialize     
      sql_by_slice = trigger.initialize_sql
      sql_by_slice.each do |slice, sql|
        execute("DROP TABLE IF EXISTS #{slice[:name]}")
        execute(sql)
      end
      replicated_table_slices(trigger.to).concat(sql_by_slice.keys)
    else
      raise "invalid action: #{action}"
    end
  end

  def create_replicated_table(table_name)
    table  = nil
    fields = []
    joins  = []

    replicated_table_slices(table_name).each do |slice|
      if table
        joins << "LEFT OUTER JOIN #{slice[:name]} ON #{table}.id = #{slice[:name]}.id"
      else
        table = slice[:name]
        fields << "#{table}.id"
      end
      fields << slice[:fields].collect {|f| "#{slice[:name]}.#{f}"}
    end

    execute %{
      CREATE TABLE #{table_name} AS
        SELECT #{fields.flatten.join(', ')}
          FROM #{table}
          #{joins.join(' ')}
    }
  end

  def replicated_table_slices(table_name = nil)
    if table_name
      replicated_table_slices[table_name.to_sym] ||= []
    else
      @replicated_table_slices ||= {}
    end
  end

  class Trigger
    attr_reader :from, :to, :key, :through_table, :through_key, :condition, :prefix, :prefix_map

    def initialize(table, opts)
      @from       = table
      @to         = opts[:to]
      @name       = opts[:name]
      @key        = opts[:key] || 'id'
      @condition  = opts[:condition] || opts[:if]
      @prefix     = opts[:prefix]
      @prefix_map = opts[:prefix_map]
      @timestamps = opts[:timestamps]
      @dependent  = opts[:dependent]

      if opts[:through]
        @through_table, @through_key = opts[:through].split('.')
        raise "through must be of the form 'table.field'" unless @through_table and @through_key
      end

      # Use opts[:prefixes] to specify valid prefixes and use the identity mapping.
      if @prefix_map.nil? and opts[:prefixes]
        @prefix_map = {}
        opts[:prefixes].each do |prefix|
          @prefix_map[prefix] = prefix
        end
      end

      if @prefix_map
        @prefix, prefix_table = @prefix.split('.').reverse
        
        if prefix_table.nil? or prefix_table == from
          @prefix_through = false
        elsif prefix_table == through_table
          @prefix_through = true
        else
          "unknown prefix table: #{prefix_table}" 
        end
      end

      @fields = {}
      opts[:fields] = [opts[:fields]] unless opts[:fields].kind_of?(Array)
      opts[:fields].each do |field|
        if field.kind_of?(Hash)
          @fields.merge!(field)
        else
          @fields[field] = field
        end
      end
    end

    def fields(opts = nil)
      if opts
        opts[:row] ||= 'ROW'
        # Add the prefixes and return an array of arrays.
        @fields.collect do |from_field, to_field|
          from_field = Array(from_field).collect {|f| "#{opts[:row]}.#{f}"}.join(" || ' ' || ") 
          to_field   = [opts[:prefix], to_field].compact.join('_')
          [from_field, to_field]
        end
      else
        # Just return the hash.
        @fields
      end
    end

    def create_sql
      %{
        CREATE OR REPLACE FUNCTION #{name}() RETURNS TRIGGER AS $$
          DECLARE
            ROW     RECORD;
            THROUGH RECORD;
          BEGIN
            IF (TG_OP = 'DELETE') THEN
              ROW := OLD;
            ELSE
              ROW := NEW;
            END IF;
            #{loop_sql}
              IF #{conditions_sql} THEN
                IF (TG_OP = 'DELETE') THEN
                  IF COUNT(*) > 0 FROM #{to} WHERE id = #{primary_key} THEN
                    #{ dependent_destroy? ? destroy_sql : update_all_sql(:indent => 18, :clear => true)}
                  END IF;
                ELSE
                  IF COUNT(*) = 0 FROM #{to} WHERE id = #{primary_key} THEN
                    #{insert_sql}
                  END IF;
                  #{update_all_sql(:indent => 18)}
                END IF;
              END IF;
            #{end_loop_sql}
            RETURN NULL;
          END;
        $$ LANGUAGE plpgsql;
        CREATE TRIGGER #{name} AFTER INSERT OR UPDATE OR DELETE ON #{from}
          FOR EACH ROW EXECUTE PROCEDURE #{name}();
      }
    end

    def destroy_sql
      "DELETE FROM #{to} WHERE id = #{primary_key};"
    end

    def initialize_sql(mode = nil)
      sql_by_slice = {}
      
      if through?
        tables = "#{from}, #{through_table}"
        join   = "#{from}.id = #{through_table}.#{through_key}"
      else
        tables = from
      end

      if prefix_map
        prefix_map.each do |prefix_value, mapping|
          slice_fields = []
          field_sql = fields(:prefix => mapping, :row => from).collect do |from_field, to_field|
            slice_fields << to_field
            "#{from_field} AS #{to_field}"
          end.join(', ')
          where_sql  = "WHERE " << [join, "#{prefix_field(true)} = '#{prefix_value}'"].compact.join(' AND ')
          table_name = "#{name}_#{mapping}"
          
          slice = {:name => table_name, :fields => slice_fields}
          sql_by_slice[slice] = %{
            CREATE TABLE #{table_name} AS
              SELECT #{primary_key(true)} AS id, #{field_sql} FROM #{tables} #{where_sql}
          }
        end
      else
        slice_fields = []
        field_sql = fields(:prefix => prefix, :row => from).collect do |from_field, to_field|
          slice_fields << to_field
          "#{from_field} AS #{to_field}"
        end.join(', ')
        where_sql  = "WHERE #{join}" if join
        
        slice = {:name => name, :fields => slice_fields}
        sql_by_slice[slice] = %{
          CREATE TABLE #{name} AS
            SELECT #{primary_key(true)} AS id, #{field_sql} FROM #{tables} #{where_sql}
        }
      end
      sql_by_slice
    end

    def drop_sql
      "DROP FUNCTION IF EXISTS #{name}() CASCADE"
    end

    def timestamps?
      @timestamps
    end

    def dependent_destroy?
      @dependent == :destroy
    end

    def through?
      not through_table.nil?
    end

    def prefix_through?
      @prefix_through
    end

  private

    def prefix_field(initialize = false)
      if initialize
        "#{prefix_through? ? through_table : from}.#{prefix}"
      else
        "#{prefix_through? ? 'THROUGH' : 'ROW'}.#{prefix}"
      end
    end

    def primary_key(initialize = false)
      if initialize
        "#{through? ? through_table : from}.#{key}"
      else
        @primary_key ||= "#{through? ? 'THROUGH' : 'ROW'}.#{key}"
      end
    end      
    
    def name
      @name ||= "replicate_#{from}_to_#{to}"
    end

    def loop_sql
      "FOR THROUGH IN SELECT * FROM #{through_table} WHERE #{through_key} = ROW.id LOOP" if through?
    end

    def end_loop_sql
      "END LOOP;" if through?
    end

    def conditions_sql
      conditions = []
      conditions << "#{primary_key} IS NOT NULL"
      conditions << "#{prefix_field} IN (#{prefixes_sql})" if prefix_map 
      conditions << condition if condition
      conditions.join(' AND ')
    end

    def prefixes_sql
      prefix_map.keys.collect {|p| "'#{p}'"}.join(',')
    end

    def insert_sql
      if timestamps?
        "INSERT INTO #{to} (id, created_at) VALUES (#{primary_key}, NOW());"
      else
        "INSERT INTO #{to} (id) VALUES (#{primary_key});"
      end
    end
      
    def update_sql(opts = {})
      updates = fields(:prefix => opts[:prefix]).collect do |from_field, to_field|
        from_field = 'NULL' if opts[:clear]
        "#{to_field} = #{from_field}"
      end
      updates << "updated_at = NOW()" if timestamps?
      sql = "SET #{updates.join(', ')}"
      sql = "UPDATE #{to} #{sql} WHERE #{to}.id = #{primary_key};"
      sql
    end

    def update_all_sql(opts = {})
      return update_sql(opts.merge(:prefix => prefix)) unless prefix_map

      sql = ''
      opts[:indent] ||= 0
      newline = "\n#{' ' * opts[:indent]}"
      cond = 'IF'
      prefix_map.each do |prefix_value, mapping|
        sql << "#{cond} #{prefix_field} = '#{prefix_value}' THEN" + newline
        sql << "  #{update_sql(opts.merge(:prefix => mapping))}" + newline 
        cond = 'ELSIF'
      end
      sql << "END IF;"
      sql
    end
  end
end
