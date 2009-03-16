options = {}
options[:port] = 11222
options[:databases] = ''
options[:config] = ''

OptionParser.new do |opts|
  opts.banner = "Usage: dynomite console [options]"

  contents =  File.read(File.dirname(__FILE__) + "/shared/common.rb")
  eval contents

end.parse!

cookie = Digest::MD5.hexdigest(options[:cluster] + "NomMxnLNUH8suehhFg2fkXQ4HVdL2ewXwM")

str = "erl -sname remsh_#{$$} -remsh #{options[:name]}@#{`hostname -s`.chomp} -hidden -setcookie #{cookie}"
puts str
exec str