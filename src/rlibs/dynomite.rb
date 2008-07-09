# ruby protocol handler for dynomite.

require 'socket'
include Socket::Constants

class DynomiteError < Exception; end

class Dynomite
  DEFAULTS = {
    :port => 11211,
    :host => '127.0.0.1'
  }
  
  
  def initialize(options={})
    options = DEFAULTS.merge(options)
    @addr = Socket.pack_sockaddr_in(options[:port], options[:host])
    @socket = Socket.new(AF_INET6, SOCK_STREAM, 0)
    @socket.connect(@addr)
    @socket.sync = true
  end
  
  def get(key)
    socket.write("get #{key.length} #{key}\n")
    command = read_section
    case command
    when "fail"
      reason = read_section
      raise DynomiteError.new(reason)
    when "succ"
      items = read_section.to_i
      ctx_length = read_section.to_i
      ctx = read_binary(ctx_length)
      data_items = []
      items.times do
        data_length = read_section.to_i
        data_items << read_binary(data_length)
      end
      [ctx, data_items]
    end
  end
  
  def put(key, context, data)
    ctx_length = context ? context.length : 0
    socket.write("put #{key.length} #{key} #{ctx_length} #{context} #{data.length} ")
    socket.write(data)
    socket.write("\n")
    command = read_section
    case command
    when "fail"
      reason = read_section
      raise DynomiteError.new(reason)
    when "succ"
      read_section.to_i
    end
  end
  
  def has_key(key)
    socket.write("has #{key.length} #{key}\n")
    command = read_section
    case command
    when "fail"
      reason = read_section
      raise DynomiteError.new(reason)
    when "yes"
      n = read_section.to_i
      [true, n]
    when "no"
      n = read_section.to_i
      [false, n]
    end
  end
  
  def delete(key)
    socket.write("del #{key.length} #{key}\n")
    command = read_section
    case command
    when "fail"
      reason = read_section
      raise DynomiteError.new(reason)
    when "succ"
      read_section.to_i
    end
  end
  
  def close
    socket.close unless socket.closed?
  end
  
  private
  
  def socket
    @socket
  end
  
  def read_section
    buff = ""
    while (socket.read(1, buff) && buff !~ /^.*\s$/); end
    buff[0,buff.length-1]
  end
  
  def read_binary(length)
    buff = socket.read(length)
    #clear terminating char
    socket.read(1)
    buff
  end
  
end