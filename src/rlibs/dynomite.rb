# ruby protocol handler for dynomite.

require 'socket'
include Socket::Constants

class Dynomite
  DEFAULTS = {
    :port => 11211,
    :host => 'localhost'
  }
  
  
  def initialize(options={})
    options = options.merge(DEFAULTS)
    @addr = Socket.pack_sockaddr_in(options[:port], options[:host])
    @socket = Socket.new(AF_INET, SOCK_STREAM, 0)
    @socket.connect(@addr)
    @socket.sync = true
  end
  
  def get(key)
    @socket.write("get #{key.length} #{key}")
    buff = ""
    while (@socket.read(1, buff) && buff =~ /^\d+$/); end
    length = buff.to_i
    buff = ""
    @socket.read(length, buff)
    buff
  end
  
  def put(key, data)
    @socket.write("put #{key.length} #{key} #{data.length}")
    @socket.write(data)
  end
  
  def has_key(key)
    @socket.write("has #{key.length} #{key}")
    
  end
  
  def delete(key)
    @socket.write("del #{key.length} #{key}")
  end
  
  private
  
  def socket
    @socket
  end
  
end