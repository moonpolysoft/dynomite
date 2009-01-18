options = {}

OptionParser.new do |opts|
  opts.banner = "Usage: dynomite stop [options]"

  contents =  File.read(File.dirname(__FILE__) + "/shared/common.rb")
  eval contents
end.parse!

cookie = Digest::MD5.hexdigest(options[:cluster] + "NomMxnLNUH8suehhFg2fkXQ4HVdL2ewXwM")

str = %Q(erl -sname remsh_#{$$} -remsh #{options[:name]}@#{`hostname -s`.chomp} -hidden -setcookie #{cookie}" -noshell -run membership leave)
puts str
exec str
