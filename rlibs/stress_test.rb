require 'rubygems'
require File.dirname(__FILE__) + "/dynomite"

def random_bytes(size)
  buff = ""
  size.times do
    buff << rand(256)
  end
  buff
end

dyn = Dynomite.new

ary = (1..10000).to_a.map do |i|
  ["key#{rand(9000)}", random_bytes(100)]
end

time = -Time.now.to_f

ary.each do |key, val|
  dyn.put key, nil, val
end

time += Time.now.to_f

puts "time taken: #{time}"