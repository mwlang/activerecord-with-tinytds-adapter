require 'rubygems'
require 'rake'
require 'rake/testtask'
require 'rake/rdoctask'
require 'rake/packagetask'
require 'rake/gempackagetask'
require 'rake/contrib/rubyforgepublisher'
require File.join(File.dirname(__FILE__), 'lib', 'active_record', 'version')

PKG_BUILD     = ENV['PKG_BUILD'] ? '.' + ENV['PKG_BUILD'] : ''
PKG_NAME      = 'activerecord'
PKG_VERSION   = ActiveRecord::VERSION::STRING + PKG_BUILD
PKG_FILE_NAME = "#{PKG_NAME}-#{PKG_VERSION}"

RELEASE_NAME  = "REL #{PKG_VERSION}"

RUBY_FORGE_PROJECT = "activerecord"
RUBY_FORGE_USER    = "webster132"

PKG_FILES = FileList[
    "lib/**/*", "test/**/*", "examples/**/*", "doc/**/*", "[A-Z]*", "install.rb", "Rakefile"
].exclude(/\bCVS\b|~$/)


desc "Default Task"
task :default => [ :test_sqlserver ]

# Run the unit tests

for adapter in %w( mysql postgresql sqlite sqlite3 firebird sqlserver sqlserver_odbc db2 oracle sybase openbase frontbase tinytds )
  Rake::TestTask.new("test_#{adapter}") { |t|
    t.libs << "test" << "test/connections/native_#{adapter}"
    if adapter =~ /^sqlserver/
      t.pattern = "test/**/*_test{,_sqlserver}.rb"
    else
      t.pattern = "test/**/*_test{,_#{adapter}}.rb"
    end
    t.verbose = true
  }
end

SCHEMA_PATH = File.join(File.dirname(__FILE__), *%w(test fixtures db_definitions))

desc 'Build the MySQL test databases'
task :build_mysql_databases do 
  %x( mysqladmin  create activerecord_unittest )
  %x( mysqladmin  create activerecord_unittest2 )
  %x( mysql -e "grant all on activerecord_unittest.* to rails@localhost" )
  %x( mysql -e "grant all on activerecord_unittest2.* to rails@localhost" )
  %x( mysql  activerecord_unittest < #{File.join(SCHEMA_PATH, 'mysql.sql')} )
  %x( mysql  activerecord_unittest < #{File.join(SCHEMA_PATH, 'mysql2.sql')} )
end

desc 'Drop the MySQL test databases'
task :drop_mysql_databases do 
  %x( mysqladmin -f drop activerecord_unittest )
  %x( mysqladmin -f drop activerecord_unittest2 )
end

desc 'Rebuild the MySQL test databases'
task :rebuild_mysql_databases => [:drop_mysql_databases, :build_mysql_databases]

desc 'Build the PostgreSQL test databases'
task :build_postgresql_databases do 
  %x( createdb -U postgres activerecord_unittest )
  %x( createdb -U postgres activerecord_unittest2 )
  %x( psql activerecord_unittest -f #{File.join(SCHEMA_PATH, 'postgresql.sql')} postgres )
  %x( psql activerecord_unittest2 -f #{File.join(SCHEMA_PATH, 'postgresql2.sql')}  postgres )
end

desc 'Drop the PostgreSQL test databases'
task :drop_postgresql_databases do 
  %x( dropdb -U postgres activerecord_unittest )
  %x( dropdb -U postgres activerecord_unittest2 )
end

desc 'Rebuild the PostgreSQL test databases'
task :rebuild_postgresql_databases => [:drop_postgresql_databases, :build_postgresql_databases]

desc 'Build the FrontBase test databases'
task :build_frontbase_databases => :rebuild_frontbase_databases 

desc 'Rebuild the FrontBase test databases'
task :rebuild_frontbase_databases do
  build_frontbase_database = Proc.new do |db_name, sql_definition_file|
    %(
      STOP DATABASE #{db_name};
      DELETE DATABASE #{db_name};
      CREATE DATABASE #{db_name};

      CONNECT TO #{db_name} AS SESSION_NAME USER _SYSTEM;
      SET COMMIT FALSE;

      CREATE USER RAILS;
      CREATE SCHEMA RAILS AUTHORIZATION RAILS;
      COMMIT;

      SET SESSION AUTHORIZATION RAILS;
      SCRIPT '#{sql_definition_file}';

      COMMIT;

      DISCONNECT ALL; 
    )
  end
  create_activerecord_unittest  = build_frontbase_database['activerecord_unittest',  File.join(SCHEMA_PATH, 'frontbase.sql')]
  create_activerecord_unittest2 = build_frontbase_database['activerecord_unittest2', File.join(SCHEMA_PATH, 'frontbase2.sql')]
  execute_frontbase_sql = Proc.new do |sql| 
    system(<<-SHELL)
    /Library/FrontBase/bin/sql92 <<-SQL
    #{sql}
    SQL
    SHELL
  end
  execute_frontbase_sql[create_activerecord_unittest]
  execute_frontbase_sql[create_activerecord_unittest2]
end

# Generate the RDoc documentation

Rake::RDocTask.new { |rdoc|
  rdoc.rdoc_dir = 'doc'
  rdoc.title    = "Active Record -- Object-relation mapping put on rails"
  rdoc.options << '--line-numbers' << '--inline-source' << '-A cattr_accessor=object'
  rdoc.template = "#{ENV['template']}.rb" if ENV['template']
  rdoc.rdoc_files.include('README', 'RUNNING_UNIT_TESTS', 'CHANGELOG')
  rdoc.rdoc_files.include('lib/**/*.rb')
  rdoc.rdoc_files.exclude('lib/active_record/vendor/*')
  rdoc.rdoc_files.include('dev-utils/*.rb')
}

# Enhance rdoc task to copy referenced images also
task :rdoc do
  FileUtils.mkdir_p "doc/files/examples/"
  FileUtils.copy "examples/associations.png", "doc/files/examples/associations.png"
end


# Create compressed packages

dist_dirs = [ "lib", "test", "examples", "dev-utils" ]

spec = Gem::Specification.new do |s|
  s.name = PKG_NAME
  s.version = PKG_VERSION
  s.summary = "Implements the ActiveRecord pattern for ORM."
  s.description = %q{Implements the ActiveRecord pattern (Fowler, PoEAA) for ORM. It ties database tables and classes together for business objects, like Customer or Subscription, that can find, save, and destroy themselves without resorting to manual SQL.}

  s.files = [ "Rakefile", "install.rb", "README", "RUNNING_UNIT_TESTS", "CHANGELOG" ]
  dist_dirs.each do |dir|
    s.files = s.files + Dir.glob( "#{dir}/**/*" ).delete_if { |item| item.include?( "\.svn" ) }
  end
  
  s.add_dependency('activesupport', '= 1.4.4' + PKG_BUILD)

  s.files.delete "test/fixtures/fixture_database.sqlite"
  s.files.delete "test/fixtures/fixture_database_2.sqlite"
  s.files.delete "test/fixtures/fixture_database.sqlite3"
  s.files.delete "test/fixtures/fixture_database_2.sqlite3"
  s.require_path = 'lib'
  s.autorequire = 'active_record'

  s.has_rdoc = true
  s.extra_rdoc_files = %w( README )
  s.rdoc_options.concat ['--main',  'README']
  
  s.author = "David Heinemeier Hansson"
  s.email = "david@loudthinking.com"
  s.homepage = "http://www.rubyonrails.org"
  s.rubyforge_project = "activerecord"
end
  
Rake::GemPackageTask.new(spec) do |p|
  p.gem_spec = spec
  p.need_tar = true
  p.need_zip = true
end

task :lines do
  lines, codelines, total_lines, total_codelines = 0, 0, 0, 0

  for file_name in FileList["lib/active_record/**/*.rb"]
    next if file_name =~ /vendor/
    f = File.open(file_name)

    while line = f.gets
      lines += 1
      next if line =~ /^\s*$/
      next if line =~ /^\s*#/
      codelines += 1
    end
    puts "L: #{sprintf("%4d", lines)}, LOC #{sprintf("%4d", codelines)} | #{file_name}"
    
    total_lines     += lines
    total_codelines += codelines
    
    lines, codelines = 0, 0
  end

  puts "Total: Lines #{total_lines}, LOC #{total_codelines}"
end


# Publishing ------------------------------------------------------

desc "Publish the beta gem"
task :pgem => [:package] do 
  Rake::SshFilePublisher.new("davidhh@wrath.rubyonrails.org", "public_html/gems/gems", "pkg", "#{PKG_FILE_NAME}.gem").upload
  `ssh davidhh@wrath.rubyonrails.org './gemupdate.sh'`
end

desc "Publish the API documentation"
task :pdoc => [:rdoc] do 
  Rake::SshDirPublisher.new("davidhh@wrath.rubyonrails.org", "public_html/ar", "doc").upload
end

desc "Publish the release files to RubyForge."
task :release => [ :package ] do
  require 'rubyforge'

  packages = %w( gem tgz zip ).collect{ |ext| "pkg/#{PKG_NAME}-#{PKG_VERSION}.#{ext}" }

  rubyforge = RubyForge.new
  rubyforge.login
  rubyforge.add_release(PKG_NAME, PKG_NAME, "REL #{PKG_VERSION}", *packages)
end