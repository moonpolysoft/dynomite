#defaults
options[:name] = 'dynomite'
options[:cluster] = 'development'

opts.separator ""
opts.separator "common options:"

opts.on("-o", "--node [NODE]", "The erlang nodename") do |name|
  options[:name] = name
end

opts.on("-c", "--cluster [CLUSTER]", "The cluster name (cookie token)") do |name|
  options[:cluster] = name
end