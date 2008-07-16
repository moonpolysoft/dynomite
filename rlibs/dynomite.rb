# ruby protocol handler for dynomite.

require 'socket'
include Socket::Constants

class DynomiteError < Exception; end

class Dynomite
  DEFAULTS = {
    :port => 11222,
    :host => 'localhost'
  }
  
  def initialize(options={})
    options = DEFAULTS.merge(options)
    @addr = Socket.pack_sockaddr_in(options[:port], options[:host])
    @socket = Socket.new(AF_INET6, SOCK_STREAM, 0)
    @socket.connect(@addr)
    @socket.sync = true
  end
  
  def get(key)
    write("get #{key.length} #{key}\n")
    command = read_section
    case command
    when "fail"
      reason = read_line
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
    write("put #{key.length} #{key} #{ctx_length} #{context} #{data.length} ")
    write(data)
    write("\n")
    command = read_section
    case command
    when "fail"
      reason = read_line
      raise DynomiteError.new(reason)
    when "succ"
      return read_section.to_i
    end
  end
  
  def has_key(key)
    write("has #{key.length} #{key}\n")
    command = read_section
    case command
    when "fail"
      reason = read_line
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
    write("del #{key.length} #{key}\n")
    command = read_section
    case command
    when "fail"
      reason = read_line
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
  
  def read(length)
    socket.read(length)
  end
  
  def write(data)
    socket.write(data)
  end
  
  def read_section
    buff = ""
    while ((char = read(1)) && char != ' ' && char != "\n")
      buff << char
    end
    buff
  end
  
  def read_line
    buff = ""
    while ((char = read(1)) && char != "\n")
      buff << char
    end
    buff
  end
  
  def read_binary(length)
    buff = read(length)
    #clear terminating char
    read(1)
    buff
  end
  
end