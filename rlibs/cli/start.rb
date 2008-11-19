options = {}
options[:port] = 11222
options[:databases] = ''
options[:config] = ''

OptionParser.new do |opts|
  opts.banner = "Usage: dynomite start [options]"

  contents =  File.read(File.dirname(__FILE__) + "/shared/common.rb")
  eval contents

  opts.separator ""
  opts.separator "Specific options:"
  
  opts.on("-b", "--blocksize [BLOCKSIZE]", "Blocksize of the hard disk") do |blocksize|
    options[:blocksize] = "-dynomite blocksize #{blocksize}"
  end
  
  opts.on("-p", "--port [PORT]", "The port to listen on") do |port|
    options[:port] = "-dynomite port #{port}"
  end

  opts.on("-t", "--thrift-port [PORT]", "The port for the thrift service to listen on") do |port|
    options[:thrift_port] = "-dynomite thrift_port #{port}"
  end
  
  opts.on("-u", "--web [PORT]", "Port for the web interface") do |port|
    options[:web_port] = "-dynomite web_port #{port}"
  end
  
  opts.on("-l", "--log [LOGFILE]", "error log path") do |log|
    options[:log] = %Q[-kernel error_logger '{file,"#{File.join(log, 'dynomite.log')}"}' -sasl sasl_error_logger '{file,"#{File.join(log, 'sasl.log')}"}']
  end
  
  opts.on('-j', "--join [NODENAME]", 'node to join with') do |node|
    options[:jointo] = %Q(-dynomite jointo "'#{node}'")
  end
  
  opts.on("-s", "--storage [MODULE]", "storage module to use") do |storage|
    options[:storage] = %Q(-dynomite storage_mod '#{storage}')
  end
  
  opts.on('-m', "--data [DATADIR]", "data directory") do |dir|
    options[:directory] = %Q(-dynomite directory '"#{dir}"' -mnesia dir '"#{dir}"')
  end
  
  opts.on('-n', "--replication [N]", "replication factor") do |n|
    options[:n] = %Q(-dynomite n #{n})
  end
  
  opts.on('-r', "--read [R]", "read factor") do |r|
    options[:r] = %Q(-dynomite r #{r})
  end
  
  opts.on('-w', "--write [W]", 'write factor') do |w|
    options[:w] = %Q(-dynomite w #{w})
  end
  
  opts.on('-q', "--partitions [Q]", 'partitions, as a power of 2') do |q|
    options[:q] = %Q(-dynomite q #{q})
  end
  
  opts.on('-d', "--detached", "run detached from the shell") do |detached|
    options[:detached] = '-detached'
  end
end.parse!

cookie = Digest::MD5.hexdigest(options[:cluster] + "NomMxnLNUH8suehhFg2fkXQ4HVdL2ewXwM")

str = "erl \
  -boot start_sasl \
  +K true \
  +A 128 \
  -smp enable \
  -pz #{ROOT}/ebin/ \
  -pz #{ROOT}/deps/mochiweb/ebin \
  -pz #{ROOT}/deps/rfc4627/ebin \
  -pz #{ROOT}/deps/thrift/ebin \
  -sname #{options[:name]} \
  #{options[:log]} \
  -noshell \
  #{options[:port]} \
  #{options[:web_port]} \
  #{options[:thrift_port]} \
  #{options[:jointo]} \
  #{options[:directory]} \
  #{options[:storage]} \
  #{options[:n]} \
  #{options[:r]} \
  #{options[:w]} \
  #{options[:q]} \
  #{options[:blocksize]} \
  -setcookie #{cookie} \
  -run dynomite start \
  #{options[:detached]}"
puts str
exec str
