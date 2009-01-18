require 'rubygems'
require 'inline'
require 'optparse'
require 'thrift'
require 'benchmark'
require 'thrift/transport/socket'
require 'thrift/protocol/tbinaryprotocolaccelerated'
require File.dirname(__FILE__) + "/../gen-rb/Dynomite"

options = {
  :concurrency => 20,
  :hosts => [],
  :size => 100,
  :ratio => 0.5,
  :keyspace => 100000,
  :logdir => File.dirname(__FILE__) + "/../../bench_log"
}

OptionParser.new do |opts|
  opts.banner = "Usage: distributed_bench [options]"
  
  opts.on('-h', '--host [HOST]', "Add another host to test against.  Should add the whole cluster.") do |host|
    options[:hosts] << host
  end
  
  opts.on('-c', '--concurrency [INT]', "the concurrency level for the test.  How many clients to start.") do |c|
    options[:concurrency] = c.to_i
  end
  
  opts.on('-r', '--ratio [R]', "the ratio of gets to puts.  0.0 means all puts, 1.0 means all gets.") do |r|
    options[:ratio] = r.to_f
  end
  
  opts.on("-l", "--log [LOGDIR]", "Where the instances should log their raw performance data.") do |l|
    options[:logdir] = l
  end
  
  opts.on("-s", "--size [SIZE]", "The size of the values to use, in bytes.") do |s|
    options[:size] = s.to_i
  end
  
  opts.on("-k", "--keyspace [KEYSPACE]", "The integer size of the keyspace.") do |k|
    options[:keyspace] = k.to_i
  end
  
end.parse!

def new_bytes(size)
  buff = ""
  size.times do
    buff << rand(256)
  end
  buff
end

FileUtils.mkdir_p options[:logdir]

concurrency = options[:concurrency]
hosts = options[:hosts]
if concurrency < hosts.length
  hosts = hosts[0..concurrency]
end

kids_p_host = concurrency / hosts.length

keyspace = options[:keyspace]
keychars = (Math.log(keyspace) / Math.log(26)).ceil
key = (0..keychars).to_a.map {|n| "a"}.join
keys = Array.new(keyspace)
keyspace.times do |n|
  keys << key.succ!.dup
end
keys.compact!

worker = lambda do |host|
  log = File.open(options[:logdir] + "/#{$$}_bench.log", "w")
  Signal.trap("INT") do
    log.close
    exit(0)
  end
  socket = Thrift::Socket.new(*host.split(":"))
  socket.open
  protocol = Thrift::BinaryProtocolAccelerated.new(
    Thrift::BufferedTransport.new(socket))
  dyn = Dynomite::Client.new(protocol)
  
  bytes = new_bytes(options[:size])
  
  while true
    begin
      index = rand(keys.length)
      key = keys[index]
      t = nil
      time = Benchmark.realtime {
        if (rand < options[:ratio])
          t = "put"
          dyn.put key, nil, bytes.succ!
        else
          t = "get"
          dyn.get key
        end
      }
      log.puts "#{Time.now.to_f}\t#{t}\t#{time}\t#{key}\t#{host}"
    rescue => boom
      log.puts "#{Time.now.to_f}\terror\t#{boom.message}\t#{key}\t#{host}"
    end
  end
end

pids = []

total_kids = 0
hosts.each do |host|
  kids_p_host.times do
    total_kids += 1
    puts "worker for #{host}"
    pids << fork { worker.call(host) }
  end
end
i=0
while total_kids < concurrency
  host = hosts[i]
  puts "worker for #{host}"
  pids << fork { worker.call(host) }
  total_kids += 1
  i += 1
end

Signal.trap("INT") do 
  pids.each {|pid| Process.kill("INT", pid)}
  exit(0)
end

Process.waitall
