require 'rubygems'
require 'optparse'

class Array
  #assumes the array is sorted
  def percentile(perc)
    len = self.length
    index = (len * perc).round
    self[index]
  end
end

options = {
  :logdir => "bench_log"
}

OptionParser.new do |opts|
  opts.banner = "Usage: analyze_bench [options]"
  
  opts.on("-l", "--log [LOGDIR]", "Where the instances should log their raw performance data.") do |l|
    options[:logdir] = l
  end
  
end.parse!

stats = []
errors = []

Dir[options[:logdir] + "/*.log"].each do |logfilename|
  File.open(logfilename, 'r') do |f|
    f.each_line do |line|
      fields = line.split("\t")
      if (fields[1] == "error")
        # errors << fields
      else
        stats << fields
      end
    end
  end
end

#the sum
gets = []
get_sum = 0
get_num = 0
puts = []
put_sum = 0
put_num = 0
stats.each do |time, op, latency, key, host|
  if op == "get"
    gets << latency.to_f
    get_sum += latency.to_f
    get_num +=1
  else
    puts << latency.to_f
    put_sum += latency.to_f
    put_num += 1
  end
end


get_avg = get_sum.to_f / get_num
put_avg = put_sum.to_f / put_num

gets.sort!
puts.sort!
get_median = gets.percentile(0.5)
put_median = puts.percentile(0.5)
get_999 = gets.percentile(0.999)
put_999 = puts.percentile(0.999)

puts "get stats: #{gets.length} datapoints"
puts "\tavg: #{get_avg}"
puts "\tmed: #{get_median}"
puts "\t99.9:#{get_999}"
puts "put stats: #{puts.length} datapoints"
puts "\tavg: #{put_avg}"
puts "\tmed: #{put_median}"
puts "\t99.9:#{put_999}"
