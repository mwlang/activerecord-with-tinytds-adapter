print "Using native TinyTDS\n"
require_dependency 'fixtures/course'
require 'logger'

ActiveRecord::Base.logger = Logger.new("debug.log")

ActiveRecord::Base.configurations = {
  'arunit' => {
    :adapter    => 'tinytds',
    :dataserver => 'localhost',
    :username   => 'sa',
    :database   => 'activerecord_unittest'
  },
  'arunit2' => {
    :adapter    => 'tinytds',
    :dataserver => 'localhost',
    :username   => 'sa',
    :database   => 'activerecord_unittest2'
  }
}

ActiveRecord::Base.establish_connection 'arunit'
Course.establish_connection 'arunit2'
