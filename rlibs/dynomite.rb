# ruby protocol handler for dynomite.

require 'socket'

class DynomiteError < Exception; end

class Dynomite
  DEFAULTS = {
    :port => 11222,
    :host => 'localhost'
  }
  
  def initialize(options={})
    options = DEFAULTS.merge(options)
    @addr = options
    connect
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
    connect if !@socket or @socket.closed?
    @socket
  end
  
  def connect
    @socket = TCPSocket.new(@addr[:host], @addr[:port])
    @socket.sync = true
    @socket
  end
  
  def read(length)
    retries = 3
    socket.read(length)
  rescue => boom
    retries -= 1
    if retries > 0
      connect
      retry
    end
  end
  
  def write(data)
    retries = 3
    socket.write(data)
  rescue => boom
    retries -= 1
    if retries > 0
      connect
      retry
    end
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