require File.dirname(__FILE__) + '/test_helper'

class ReplicateTest < Test::Unit::TestCase
  def test_sql_update_all
    t = Replicate::Trigger.new :locations,
      :to         => :users,
      :fields     => [:latitude, :longitude, {[:city, :state, :country] => :location}],
      :through    => 'events.address_id',
      :key        => 'user_id',
      :prefix     => 'events.type',
      :prefix_map => {'BirthEvent' => 'birth', 'GraduationEvent' => 'grad'}

    sql = t.create_sql
    # This is not the best way to test this. I should actually add the triggers and check that they work.
    assert_match 'FOR THROUGH IN SELECT * FROM events WHERE address_id = ROW.id LOOP', sql
    assert_match 'INSERT INTO users (id) VALUES (THROUGH.user_id)', sql
    ['birth', 'grad'].each do |prefix|
      assert_match "#{prefix}_location = ROW.city || ' ' || ROW.state || ' ' || ROW.country", sql
      assert_match "#{prefix}_latitude = ROW.latitude", sql
      assert_match "#{prefix}_longitude = ROW.longitude", sql
    end
  end
end
