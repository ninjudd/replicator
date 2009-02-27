require "digest/md5"

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
    attr_reader :from, :to, :fields, :prefix, :prefixes, :using, :through

    def initialize(table, opts)
      @from       = table
      @to         = opts[:to]
      @prefix     = opts[:prefix]
      @prefixes   = opts[:prefixes]
      @using      = opts[:using]
      @through    = opts[:through]
      @timestamps = opts[:timestamps]

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
        CREATE OR REPLACE FUNCTION #{function_name}() RETURNS TRIGGER AS $$
          DECLARE
            THROUGH RECORD;
          BEGIN
            IF (TG_OP = 'DELETE') THEN
              NEW.id   := OLD.id;
              NEW.type := OLD.type;
            END IF;
            #{loop_sql}
              IF COUNT(*) = 0 FROM #{to} WHERE id = #{using} THEN
                #{insert_sql}
              END IF;            
              #{update_all_sql(:indent => 14)}
            #{end_loop_sql}
            RETURN NEW;
          END;
        $$ LANGUAGE plpgsql;
        CREATE TRIGGER #{function_name} BEFORE INSERT OR UPDATE OR DELETE ON #{from}
          FOR EACH ROW EXECUTE PROCEDURE #{function_name}();
      }
    end

    def drop_sql
      "DROP FUNCTION #{function_name}() CASCADE"
    end

    def timestamps?
      @timestamps
    end

  private

    def function_name
      hash = Digest::MD5.hexdigest("#{fields.join(',')}:#{through}:#{using}:#{prefix}:#{prefixes}")[0,10]
      "replicate_#{from}_to_#{to}_#{hash}"
    end

    def loop_sql
      "FOR THROUGH IN #{through} LOOP" if through
    end

    def end_loop_sql
      "END LOOP;" if through
    end

    def insert_sql
      if timestamps?
        "INSERT INTO #{to} (id, created_on) VALUES (#{using}, NOW());"
      else
        "INSERT INTO #{to} (id) VALUES (#{using});"
      end
    end
      
    def update_sql(opts = {})
      prefix = opts[:prefix] + '_' if opts[:prefix]
      updates = fields.collect do |from_field, to_field|
        from_field = Array(from_field).collect {|f| "NEW.#{f}"}.join(" || ' ' || ") 
        "#{prefix}#{to_field} = #{from_field}"
      end
      updates << "updated_on = NOW()" if timestamps?
      "UPDATE #{to} SET #{updates.join(', ')} WHERE #{to}.id = #{using};"
    end

    def update_all_sql(opts = {})
      return update_sql(:prefix => prefix) unless prefixes

      sql = ''
      opts[:indent] ||= 0
      newline = "\n#{' ' * opts[:indent]}"
      cond = 'IF'
      prefixes.each do |value, mapping|
        sql << "#{cond} #{prefix} = '#{value}' THEN" + newline
        sql << "  #{update_sql(:prefix => mapping)}" + newline 
        cond = 'ELSIF'
      end
      sql << "END IF;"
      sql
    end
  end
end
