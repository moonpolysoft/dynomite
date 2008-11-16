require 'rubygems'
require 'inline'
require 'thrift'
require 'thrift/transport/socket'
require 'thrift/protocol/tbinaryprotocolaccelerated'
$:.unshift "/p/share/dynomite_rpc/gen-rb"
require '/p/share/dynomite_rpc/gen-rb/Dynomite'

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


options = {}

OptionParser.new do |opts|
  opts.banner = "Usage: dynomite bench [options]"
  contents =  File.read(File.dirname(__FILE__) + "/shared/common.rb")
  eval contents

  opts.separator ""
  opts.separator "Specific options:"
  opts.on("-m", "--data DIR", "data directory") do |dir|
    options[:data] = dir
  end
end.parse!


results = {:put => {}, :get => {}}

%w(tc_storage fs_storage couch_storage dets_storage).each do |engine|
  results[engine] = []
  5.times do |i|
    FileUtils.rm_r(options[:data]) if File.exists?(options[:data])
    size = 100*10**i
    puts "#{engine} at #{size} bytes"
    pid = fork do
      STDIN.reopen "/dev/null"
      STDOUT.reopen "/tmp/dyn.std.log"
      STDERR.reopen STDOUT
      exec "#{ROOT}/bin/dynomite start -m #{options[:data]} -o dynomite#{$$} -n 1 -w 1 -r 1 -q 0 -s #{engine} -l /tmp"
    end
    sleep(7)
    socket = Thrift::Socket.new('127.0.0.1', 9200)
    socket.open
    protocol = Thrift::BinaryProtocolAccelerated.new(Thrift::BufferedTransport.new(socket))
    dyn = Dynomite::Client.new(protocol)
    buff = new_bytes(size)
    time = -Time.now.to_f
    ary = (1..1000).to_a.map do |i|
      begin
        key = "key#{rand(9000)}"
        # puts key
        dyn.put key, nil, random_bytes(size, buff)
      rescue => boom
        puts boom.message
        socket.open unless socket.open?
      end
    end
    time += Time.now.to_f
    results[:put][engine] << [size, time]
    time = -Time.now.to_f
    ary = (1..1000).to_a.map do |i|
      begin
        key = "key#{rand(9000)}"
        # puts key
        dyn.get key
      rescue => boom
        puts boom.message
        socket.open unless socket.open?
      end
    end
    time += Time.now.to_f
    results[:get][engine] << [size, time]
    Process.kill("KILL", pid)
    Process.waitpid(pid)
  end
end

[:get, :put].each do |type|
  results[type].each do |k, ary|
    mapped = ary.map do |bytes, time|
      [bytes, time]
    end
    puts "#{k}_#{type} =\t#{mapped.inspect}"
  end
end
# at_exit {
#   Process.kill("INT", File.read("/tmp/dynomite.pid").to_i)
# }