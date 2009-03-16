require 'rubygems'
require 'ostruct'
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
  
  opts.on("-a", "--all [ALLDIR]", "Analyze all directories and spit out csv info") do |a|
    options[:all] = a
  end
  
end.parse!


def get_stats(dir)
  stats = []
  error_count = 0
  
  Dir[dir + "/*.log"].each do |logfilename|
    File.open(logfilename, 'r') do |f|
      f.each_line do |line|
        fields = line.split("\t")
        if (fields[1] == "error")
          error_count += 1
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
  errors = 0
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
  stats.sort! {|a, b| a.first <=> b.first }

  get_avg = get_sum.to_f / get_num
  put_avg = put_sum.to_f / put_num

  gets.sort!
  puts.sort!
  get_median = gets.percentile(0.5)
  put_median = puts.percentile(0.5)
  get_999 = gets.percentile(0.999)
  put_999 = puts.percentile(0.999)
  OpenStruct.new(
    :dir => dir,
    :min_time => stats.first[0].to_f,
    :max_time => stats.last[0].to_f,
    :get_length => gets.length,
    :get_avg => get_avg,
    :get_median => get_median,
    :get_999 => get_999,
    :put_length => puts.length,
    :put_avg => put_avg,
    :put_median => put_median,
    :put_999 => put_999,
    :error_count => error_count
  )
end

if options[:all]
  puts "dir,get_length,get_avg,get_median,get_999,put_length,put_avg,put_median,put_999,error_count"
  directories = Dir[options[:all] + "/*"].each do |dir|
    stats = get_stats(dir)
    STDOUT.write "#{stats.dir},#{stats.get_length},#{stats.get_avg},#{stats.get_median},#{stats.get_999},"
    puts "#{stats.put_length},#{stats.put_avg},#{stats.put_median},#{stats.put_999},#{stats.error_count}"
  end
else
  stats = get_stats(options[:logdir])
  puts "errors #{stats.error_count}"
  puts "get stats: #{stats.get_length} datapoints"
  puts "\tavg: #{stats.get_avg}"
  puts "\tmed: #{stats.get_median}"
  puts "\t99.9:#{stats.get_999}"
  puts "put stats: #{stats.put_length} datapoints"
  puts "\tavg: #{stats.put_avg}"
  puts "\tmed: #{stats.put_median}"
  puts "\t99.9:#{stats.put_999}"
  puts "#{(stats.put_length + stats.get_length) / (stats.max_time - stats.min_time)} reqs/s"
end