require 'rubygems'
require 'inline'
require File.dirname(__FILE__) + "/dynomite"

Kernel.inline do |builder|
  builder.c_raw <<-EOF
    static VALUE random_bytes(int argc, VALUE *argv, VALUE self) {
      int size = FIX2INT(argv[0]);
      VALUE buff = argv[1];
      int i;
      for(i=0; i<size; i++) {
        char random_byte = (char)rand();
        RSTRING(buff)->ptr[i] = random_byte;
      }
      return buff;
    }
  EOF
end

def new_bytes(size)
  buff = ""
  size.times do
    buff << rand(256)
  end
  buff
end

time = -Time.now.to_f

dyn = Dynomite.new :port => ARGV.shift.to_i
buff = new_bytes(300000)
ary = (1..1000).to_a.map do |i|
  key = "key#{rand(9000)}"
  puts key
  dyn.put key, nil, random_bytes(300000, buff)
end

time += Time.now.to_f

puts "time taken: #{time}"