require File.dirname(__FILE__) + '/test_helper'

class ReplicateTest < Test::Unit::TestCase
  def test_sql_update_all
    t = Replicate::Trigger.new :events,
      :to      => :search_events,
      :fields  => {:start_year => :year},
      :prefix  => {'BirthEvent' => 'birth', 'GraduationEvent' => 'grad'},
      :using   => 'NEW.owner_id'

puts t.sql_trigger
    assert_equal '', t.sql_trigger

  end
end
