require 'spec'
require 'mocha'
require 'stubba'
require File.dirname(__FILE__) + "/../rlibs/dynomite"

Spec::Runner.configure do |config|
  # config.before(:all) {}
  config.before(:each) {
    
  }
  # config.after(:all) {}
  config.after(:each) {
    
  }
end


def load_spec(filename)
  YAML.load_file(File.dirname(__FILE__) + "/#{filename}")
end

class Object
  class Bypass
    instance_methods.each do |m|
      undef_method m unless m =~ /^__/
    end

    def initialize(ref)
      @ref = ref
    end
  
    def method_missing(sym, *args)
      @ref.__send__(sym, *args)
    end
  end
  
  class Assigns
    instance_methods.each do |m|
      undef_method m unless m =~ /^__/
    end
    
    def initialize(ref)
      @ref = ref
    end
    
    def method_missing(sym, *args)
      if sym.to_s =~ /^(.+)=$/
        @ref.instance_variable_set("@#{$1}", args.length == 1 ? args.first : args)
      else
        @ref.instance_variable_get("@#{sym}")
      end
    end
  end

  def bypass
    Bypass.new(self)
  end
  
  def assigns
    Assigns.new(self)
  end
end