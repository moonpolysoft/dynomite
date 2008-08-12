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

time = -Time.now.to_f

ary = (1..500).to_a.map do |i|
  ["key#{i}", random_bytes(100)]
end

ary.each do |key, val|
  dyn.put key, nil, val
end

time += Time.now.to_f

puts "time taken: #{time}"