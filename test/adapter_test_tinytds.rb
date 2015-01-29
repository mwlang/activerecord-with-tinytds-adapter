require 'abstract_unit'
require 'fixtures/default'
require 'fixtures/post'
require 'fixtures/task'
require 'fixtures/topic'
require 'fixtures/subscriber'
require 'fixtures/joke'
require 'fixtures/binary'

if current_adapter?(:TinyTdsAdapter)

  class TinyTdsAdapterTest < Test::Unit::TestCase
    # fixtures :posts, :tasks

    def setup
      @connection = ActiveRecord::Base.connection
    end

    def test_select_value
      assert_equal 'activerecord_unittest', @connection.select_value('select DB_NAME()')
    end

    def test_current_database
      assert_equal 'activerecord_unittest', @connection.current_database
    end
  
    def test_execute_without_block_closes_statement
      @connection.execute("SELECT 1")
    end
  
    def test_execute_with_block_closes_statement
      @connection.execute("SELECT 1")
    end
  
    def test_tables
      assert_equal ["authors", "tasks", "tags"].sort, (@connection.tables & ["authors", "tasks", "tags"]).sort
    end
  
    def test_identity_column
      assert_equal "id", @connection.send(:identity_column, "accounts")
    end
  
    def test_insert_with_identity
      assert_equal 999, @connection.insert("INSERT INTO accounts ([id], [firm_id],[credit_limit]) values (999, 1, 50)")
    end
  
    def test_insert_without_identity
      last_id = @connection.insert("INSERT INTO accounts ([firm_id],[credit_limit]) values (1, 50)")
      assert_equal last_id + 1, @connection.insert("INSERT INTO accounts ([firm_id],[credit_limit]) values (1, 50)")
    end
  
    def test_execute_calls_insert
      assert_equal 999, @connection.insert("INSERT INTO accounts ([id], [firm_id],[credit_limit]) values (999, 1, 50)")
    end

    def test_can_insert_and_retrieve_international_characters
      title = "FåfæØå´øåæø" + "FåFåfæØå´øåæøåäöÅÄÖ"
      id = @connection.insert("INSERT into funny_jokes ([name]) values ('#{title}')")
      assert_equal title, @connection.select_value("select name from funny_jokes where [id] = #{id}")
    end
  
    def test_can_iaric
      name = "FåFåfæØå´øåæøåäöÅÄÖ"
      joke = Joke.create('name' => name)
      joke.reload
      assert_equal name, joke.name
    end

    def test_detects_image_col
      assert_equal :binary, @connection.columns("binaries").detect{|d| d.name == 'data'}.type
    end
  
    def test_updates
      @connection.insert("INSERT into funny_jokes ([name]) values ('one')")
      @connection.insert("INSERT into funny_jokes ([name]) values ('two')")
      assert_equal ["Knock knock", "The \\n Aristocrats\nAte the candy\n", "one", "two"], @connection.select_values('select [name] from funny_jokes')
      assert_equal 4, @connection.select_value('select count(*) as c from funny_jokes')
      assert_equal 1, @connection.update("UPDATE funny_jokes set [name] = 'three' where [name] = 'one'")
      assert_equal 3, @connection.update("UPDATE funny_jokes set [name] = 'three' where not [name] = 'three'")
      assert_equal ['three', 'three', 'three', 'three'], @connection.select_values('select [name] from funny_jokes')
    end

    def test_select
      expected = [{"name"=>"Knock knock", "id"=>1}, {"name"=>"The \\n Aristocrats\nAte the candy\n", "id"=>2}]
      assert_equal expected, @connection.send(:select, "select * from funny_jokes")
    end
  
    def test_deletes
      assert_equal 2, @connection.delete("DELETE from funny_jokes")
      assert_equal 0, @connection.select_value('select count(*) as c from funny_jokes')
    end
  
    def test_active_closes_statement
      @connection.active?
    end 

    def test_default_value
      approved_col = @connection.columns("topics").detect{|d| d.name == 'approved'}
      assert_equal true, approved_col.default
    end
  
    def test_quote_chars
      str = 'The Narrator'
      topic = Topic.create(:author_name => str)
      assert_equal str, topic.author_name
    
      topic = Topic.find_by_author_name(str.chars)
    
      assert_kind_of Topic, topic
      assert_equal str, topic.author_name, "The right topic should have been found by name even with name passed as Chars"
    end

    def test_pk
      assert "nick", Subscriber.primary_key

      subscriber = Subscriber.new
      column = subscriber.column_for_attribute("nick")
      assert "nick", column.name

      subscriber.id = "jdoe"
      assert_equal("jdoe", subscriber.nick)
      assert_equal("jdoe", subscriber.send(:read_attribute, "nick"))
      assert_equal("jdoe", subscriber.id)

      subscriber.name = "John Doe"
      assert_nothing_raised { subscriber.save! }
      assert_equal("jdoe", subscriber.id)
    end
  end
end