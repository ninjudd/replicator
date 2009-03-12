module Replicate
  def replicate(table, opts)
    action = opts.delete(:action) || :create
    trigger = Trigger.new(table, opts)

    case action
    when :create
      execute( trigger.create_sql )
    when :drop
      execute( trigger.drop_sql )
    else
      raise "invalid action: #{action}"
    end
  end

  class Trigger
    attr_reader :from, :to, :fields, :key, :through, :condition, :prefix, :prefix_map

    def initialize(table, opts)
      @from       = table
      @to         = opts[:to]
      @name       = opts[:name]
      @key        = opts[:key] || 'id'
      @through    = opts[:through]
      @condition  = opts[:condition] || opts[:if]
      @prefix     = opts[:prefix]
      @prefix_map = opts[:prefix_map]
      @timestamps = opts[:timestamps]

      # Use opts[:prefixes] to specify valid prefixes and use the identity mapping.
      if @prefix_map.nil? and opts[:prefixes]
        @prefix_map = {}
        opts[:prefixes].each do |prefix|
          @prefix_map[prefix] = prefix
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
                IF COUNT(*) = 0 FROM #{to} WHERE id = #{primary_key} THEN
                  #{insert_sql}
                END IF;
                IF (TG_OP = 'DELETE') THEN
                  #{update_all_sql(:indent => 18, :clear => true)}
                ELSE
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

    def drop_sql
      "DROP FUNCTION IF EXISTS #{name}() CASCADE"
    end

    def timestamps?
      @timestamps
    end

  private

    def primary_key
      @primary_key ||= "#{through ? 'THROUGH' : 'ROW'}.#{key}"
    end

    def name
      @name ||= "replicate_#{from}_to_#{to}"
    end

    def loop_sql
      "FOR THROUGH IN #{through} LOOP" if through
    end

    def end_loop_sql
      "END LOOP;" if through
    end

    def conditions_sql
      conditions = []
      conditions << "#{primary_key} IS NOT NULL"
      conditions << "#{prefix} IN (#{prefixes_sql})" if prefix_map 
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
      updates = fields.collect do |from_field, to_field|
        from_field = opts[:clear] ? 'NULL' : Array(from_field).collect {|f| "ROW.#{f}"}.join(" || ' ' || ") 
        field = [opts[:prefix], to_field].compact.join('_')
        "#{field} = #{from_field}"
      end
      updates << "updated_at = NOW()" if timestamps?
      "UPDATE #{to} SET #{updates.join(', ')} WHERE #{to}.id = #{primary_key};"
    end

    def update_all_sql(opts = {})
      return update_sql(opts.merge(:prefix => prefix)) unless prefix_map

      sql = ''
      opts[:indent] ||= 0
      newline = "\n#{' ' * opts[:indent]}"
      cond = 'IF'
      prefix_map.each do |prefix_value, mapping|
        sql << "#{cond} #{prefix} = '#{prefix_value}' THEN" + newline
        sql << "  #{update_sql(opts.merge(:prefix => mapping))}" + newline 
        cond = 'ELSIF'
      end
      sql << "END IF;"
      sql
    end
  end
end
