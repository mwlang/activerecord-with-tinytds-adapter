# ActiveRecord 1.15.6 with TinyTDS adapter for Rails 1.2.x 

This version of ActiveRecord is a cloned copy of 1.15.6 to which a tinytds adapter has been injected.  There is also some code added to make this library work with Rails 1.2.x running under Ruby 1.8.7.

Essentially, this implementation is a mashup of the original SQLServer adapter and very early versions of the activerecord-sqlserver-adapter.  This version was arrived at by removing ODBC and injecting TinyTDS and then working through the activerecord unit tests to get the large majority of unit tests working. Almost all unit tests pass except some of the complex sub-select queries with ordering specified.  More tests actually pass with this implementation of tinytds than did with the sqlserver adapter (which I was able to test on my mac, not the client's new Linux server).

## Use Case

Many asked, "why didn't you just port the Rails app to Rails 4?"  The simple answer: Too many Lines of Code and not enough time.  The application was hosted on Windows server and was unstable.  All of the developers intimately familiar with the project's code were long gone and we needed to help the client get to a Linux platform and fast.  We were unable to bring up a pristine Ruby 1.8.6 environment with all the older natively compiled libraries and gems.  However, we were able to bring up a pristine Ruby 1.8.7 environment to which all the gems used by the project would compile, except for one:  Ruby ODBC.  Too many things had changed with modern Linux OS and compiling and linking older C libraries was proving an insurmountable hurdle.

Implementing a tinytds driver for Rails 1.2 and porting Rails itself to Ruby 1.8.7 became the most economical and fastest route to stabilizing the client's system and that has led to this repo.  This project is not really something I intend to support and nurture as it is viewed as a crutch and stepping stone to get my clients to the next step.  However, this is not the first Rails 1.x project backed by SQL Server that I have had to rescue, so I'm making the code public to help others facing similar situations.  If you find it useful and fix any bugs, please submit a Pull Request along with supporting tests and I will merge.

## How to Use

The best way to put it to use is to vendor your rails gems and then git clone this repo to the project's ~/vendor/rails/activerecord folder.  If you're still on Ruby 1.8.6, it should work, but I did only minimal testing in that environment.  This project's definitely more heavily tested in Ruby 1.8.7, which was released *after* Rails 1.x series.  You'll find the following block of code useful to add to your application's environment.rb file if you're likewise porting your project to Ruby 1.8.7:

    if RUBY_VERSION == '1.8.7' && String.new.chars.is_a?(Enumerable::Enumerator)
      class ::String 
        alias :real_chars :chars
        def chars 
          ActiveSupport::Multibyte::Chars.new(self)
        end 
      end
    end
    
## database.yml

The database.yml file supports the following settings:

* :database
* :username
* :password
* :dataserver 
* :host
* :port
* :timeout (0 for no timeout)
* :log_ddl (true or false)

You should use either :dataserver or :host and :port combo.  If :dataserver is present, :host and :port are ignored. See TinyTDS's README for explanation and correct usage of the above settings.
