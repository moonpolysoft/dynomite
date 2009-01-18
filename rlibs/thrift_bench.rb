require 'rubygems'
require 'inline'
require 'thrift'
require 'benchmark'
require 'thrift/transport/socket'
require 'thrift/protocol/tbinaryprotocolaccelerated'
require File.dirname(__FILE__) + "/../gen-rb/Dynomite"

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


port = ARGV.shift.to_i
puts port
socket = Thrift::Socket.new('127.0.0.1', port)
socket.open
protocol = Thrift::BinaryProtocolAccelerated.new(
  Thrift::BufferedTransport.new(socket))

dyn = Dynomite::Client.new(protocol)

buff = new_bytes(1000)
ary = (1..1000).to_a.map do |i|
  key = "key#{rand(9000)}"
  puts key
  dyn.put key, nil, random_bytes(1000, buff)
end

time += Time.now.to_f

puts "time taken: #{time}"